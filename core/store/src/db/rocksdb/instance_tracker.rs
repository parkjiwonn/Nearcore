use std::sync::{Condvar, Mutex};
use tracing::info;

/// Describes number of RocksDB instances and sum of their max_open_files.
/// RocksDB 인스턴스 수와 최대 오픈 파일의 합계를 설명한다.
/// This is modified by [`InstanceTracker`] which uses RAII to automatically
/// update the trackers when new RocksDB instances are opened and closed.
/// 새로운 RocksDB 인스턴스가 열리고 닫힐때 RAII를 사용해 트래커를 자동으로 업데이트하는 [`InstanceTracker`]에 의해 수정된다.
#[derive(Default)]
struct Inner {
    /// Number of RocksDB instances open.
    /// 열린 RocksDB 인스턴스의 수
    count: usize,

    /// Sum of max_open_files configuration option of all the RocksDB instances.
    /// RocksDB 인스턴스의 최대 열기 파일 구성 옵션의 합계이다.
    /// This is used when opening multiple instances to guarantee that process
    /// limit is high enough to allow all of the instances to open the maximum
    /// number of files allowed in their respective configurations.
    /// 이 옵션은 여러 인스턴스를 열 때 모든 인스턴스가 각각의 구성에서 허용되는 최대 파일 수를 열수 있도록
    /// 프로세스 제한이 충분히 높은지 확인하기 위해 사용된다.
    /// max_open_files : 최대 열 수 있는 파일
    /// 프로세스가 OS에 요청할 수 있는 최대 Open 가능한 파일 개수
    ///
    /// For example, if NOFILE limit is set to 1024 and we try to open two
    /// RocksDB instances with max_open_files set to 512 the second open shouldF
    /// fail since the limit is too low to accommodate both databases and other
    /// file descriptor node is opening.
    /// 예를 들어, NoFIle 제한이 1024로 설정되어 있고 max_open_files가 512로 설정된 두 개의 RocksDB인스턴스를 열려고 하면
    /// 제한이 너무 낮아 DB와 다른 파일 설명자 노드를 모두 수용하지 못하기 때문에 두번째 열기는 실패하게 된다. ..?
    /// NOFILE : 열 수 있는 최대 파일 갯수 - 리눅스에선 모든 개체를 파일로 본다.
    ///
    /// Note that there is a bit of shenanigans around types.  We’re using `u64`
    /// here but `u32` in [`crate::config::StoreConfig`].  The latter uses `u32`
    /// because we don’t want to deal with `-1` having special meaning in
    /// RocksDB so we opted to for unsigned type but then needed to limit it
    /// since RocksDB doesn’t accept full unsigned range.  Here, we’re using
    /// `u64` because that’s what ends up being passed to setrlimit so we would
    /// need to cast anyway and it allows us not to worry about summing multiple
    /// limits from StoreConfig.
    /// -> RocksDB에서 -1는 특별한 의미를 갖는다.
    /// u32 : 부호 없는 32비트 정수 / u64 : 부호 없는 64비트 정수
    /// StoreConfig에서 max_open_files u32로 되어있음.
    /// -1 값을 피하기 위해 u32 타입 사용하지만 최대값을 -1보다 작게 설정함.
    /// 근데 u64는 u32보다 허용되는 값의 범위가 훨씬 늘어난다.
    /// setrlmit함수에서는 최대 열린 파일 개수에 대한 제한을 설정해야하니까 범위가 더 넓은 u64를 선택함.
    /// StorageConfig에서 여러 제한을 합산하는거 걱정하지 X
    max_open_files: u64,
}

/// Synchronisation wrapper for accessing [`Inner`] singleton.
/// ['Inner'] 싱글톤에 접근하기 위한 동기화 wrapper 이다.
/// (1) 싱글톤? 인스턴스를 하나만 생성해 사용하는 디자인 패턴
/// 인스턴스가 필요하면 새로운걸 만드는 것이 아니라 기존의 인스턴스를 사용한다.
/// (2) Synchronisation wrapper? 동기화 wrapper
/// 하나의 객체에 여러개의 스레드가 접근하려할 수 없도록 제어하고 안전한 동시 접근을 보장한다.
/// 정리 : 하나에 객체 즉 자원에 여러개의 스레드가 한번에 접근하려고 하면 충돌 문제가 발생할 수 있다.
/// 이런 문제점을 방지하기 위해 한번에 하나의 스레드가 접근하게 하기 위함. 그래서 동기화란 단어를 사용한 것.
struct State {
    inner: Mutex<Inner>,
    /// 동기화 wrapper는 뮤텍스(Mutex) 세마포어(Semaphore) 락(Lock) 등의 동기화 기법 사용
    /// State 구조체에서는 Mutex 사용
    /// Mutex 설명
    /// = 공유된 데이터를 보호하는데 유용한 상호 제외 프리미티브
    /// 같은 자원에 접근하는 스레드들이 겹치지 않고 단독으로 접근할 수 있도록 상호 배제되도록 하는 기술임.
    /// 특징 : 동기화 대상이 하나임.
    zero_cvar: Condvar, /// Convar : condition value 조건 변수
    /// 설명 : Blocks the current thread until this condition variable receives a notification.
    /// 이 조건 변수가 알림을 수신받을 때까지 현재 스레드를 차단하는 것.
    /// 정리 : 특정 조건이 발생할 때 까지 스레드를 대기시키는 것
}

impl State {
    /// Creates a new instance with counts initialised to zero.
    /// 카운트가 0으로 초기화된 새 인스턴스를 생성한다.
    const fn new() -> Self {
        let inner = Inner { count: 0, max_open_files: 0 };
        Self { inner: Mutex::new(inner), zero_cvar: Condvar::new() }
    }

    /// Registers a new RocksDB instance and checks if limits are enough for
    /// given max_open_files configuration option.
    /// 새 RocksDB 인스턴스를 등록하고 주어진 max_open_files 구성 옵션에 대한 제한이 충분한지 확인힌다.
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.
    /// 프로세스의 리소스 제한이 너무 낮아 새로운 db 인스턴스에 대한 파일 생성자를 max_open_files가 열지못한다면 에러를 반환한다.
    fn try_new_instance(&self, max_open_files: u32) -> Result<(), String> {
        let num_instances = {
            let mut inner = self.inner.lock().unwrap();
            let max_open_files = inner.max_open_files + u64::from(max_open_files);
            ensure_max_open_files_limit(RealNoFile, max_open_files)?; /// max_open_files의 제한을 확인한다.
            inner.max_open_files = max_open_files;
            inner.count += 1;
            inner.count
            /// try_new_instance를 거치면 인스턴스가 하나씩 생기는 거니가 count에 +1 씩 해주는 것.
        };
        /// 새로운 RocksDB 인스턴스를 연다.
        info!(target: "db", num_instances, "Opened a new RocksDB instance.");
        Ok(())
    }

    fn close_instance(&self, max_open_files: u32) {
        let num_instances = {
            let mut inner = self.inner.lock().unwrap();

            inner.max_open_files = inner.max_open_files.saturating_sub(max_open_files.into());
            inner.count = inner.count.saturating_sub(1);
            /// =====================위 두개 코드는 파일 제한 감소시키고 count 감소시키는거 같은데....

            if inner.count == 0 {
                self.zero_cvar.notify_all();
                /// notify_all : 조건 변수에 있는 모든 현재 대기자가 깨어날 수 있도록 함.
            }
            inner.count
        };
        /// RocksDB 인스턴스를 닫는다.
        info!(target: "db", num_instances, "Closed a RocksDB instance.");
    }

    /// Blocks until all RocksDB instances (usually 0 or 1) shut down.
    /// 모든 RocksDB 인스턴스가 ( 0 아니면 1이 ) 중지될 때 까지 차단한다.
    fn block_until_all_instances_are_closed(&self) {
        let mut inner = self.inner.lock().unwrap();
        while inner.count != 0 {
            info!(target: "db", num_instances=inner.count,
                  "Waiting for remaining RocksDB instances to close");
            inner = self.zero_cvar.wait(inner).unwrap();
        }
        info!(target: "db", "All RocksDB instances closed.");
    }
}

/// The [`State`] singleton tracking all opened RocksDB instances.
static STATE: State = State::new();

/// Blocks until all RocksDB instances (usually 0 or 1) shut down.
pub(super) fn block_until_all_instances_are_closed() {
    STATE.block_until_all_instances_are_closed();
}

/// RAII style object which updates the instance tracker stats.
///
/// We’ve seen problems with RocksDB corruptions.  InstanceTracker lets us
/// gracefully shutdown the process letting RocksDB to finish all operations and
/// leaving the instances in a valid non-corrupted state.
pub(super) struct InstanceTracker {
    /// max_open_files configuration of given RocksDB instance.
    max_open_files: u32,
}

impl InstanceTracker {
    /// Registers a new RocksDB instance and checks if limits are enough for
    /// given max_open_files configuration option.
    ///
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.  On
    /// success max_open_files descriptors are ‘reserved’ for this instance so
    /// that creating more instances will take into account the sum of all the
    /// max_open_files options.
    ///
    /// The instance is unregistered once this object is dropped.
    pub(super) fn try_new(max_open_files: u32) -> Result<Self, String> {
        STATE.try_new_instance(max_open_files)?;
        Ok(Self { max_open_files })
    }
}

impl Drop for InstanceTracker {
    /// Deregisters a RocksDB instance and frees its reserved NOFILE limit.
    ///
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.
    ///
    /// The instance is unregistered once this object is dropped.
    fn drop(&mut self) {
        STATE.close_instance(self.max_open_files);
    }
}

/// Ensures that NOFILE limit can accommodate `max_open_files` plus some small
/// margin of file descriptors.
///
/// A RocksDB instance can keep up to the configured `max_open_files` number of
/// file descriptors.  In addition, we need handful more for other processing
/// (such as network sockets to name just one example).  If NOFILE is too small
/// opening files may start failing which would prevent us from operating
/// correctly.
///
/// To avoid such failures, this method ensures that NOFILE limit is large
/// enough to accommodate `max_open_file` plus another 1000 file descriptors.
/// If current limit is too low, it will attempt to set it to a higher value.
///
/// Returns error if NOFILE limit could not be read or set.  In practice the
/// only thing that can happen is hard limit being too low such that soft limit
/// cannot be increased to required value.
fn ensure_max_open_files_limit(mut nofile: impl NoFile, max_open_files: u64) -> Result<(), String> {
    let required = max_open_files.saturating_add(1000);
    if required > i64::MAX as u64 {
        return Err(format!(
            "Unable to change limit for the number of open files (NOFILE): \
             Required limit of {required} is too high"
        ));
    };
    let (soft, hard) = nofile.get().map_err(|err| {
        format!("Unable to get limit for the number of open files (NOFILE): {err}")
    })?;
    if required <= soft {
        Ok(())
    } else if required <= hard {
        nofile.set(required, hard).map_err(|err| {
            format!(
                "Unable to change limit for the number of open files (NOFILE) \
                 from ({soft}, {hard}) to ({required}, {hard}): {err}"
            )
        })
    } else {
        Err(format!(
            "Hard limit for the number of open files (NOFILE) is too low \
             ({hard}).  At least {required} is required.  Set ‘ulimit -Hn’ \
             accordingly and restart the node."
        ))
    }
}

/// Interface for accessing the NOFILE resource limit.
///
/// The essentially trait exists for testing only.  It allows
/// [`ensure_max_open_files_limit`] to be parameterised such
/// that it access mock limits rather than real ones.
trait NoFile {
    fn get(&self) -> std::io::Result<(u64, u64)>;
    fn set(&mut self, soft: u64, hard: u64) -> std::io::Result<()>;
}

/// Access to the real process NOFILE resource limit.
struct RealNoFile;

impl NoFile for RealNoFile {
    fn get(&self) -> std::io::Result<(u64, u64)> {
        rlimit::Resource::NOFILE.get()
    }

    fn set(&mut self, soft: u64, hard: u64) -> std::io::Result<()> {
        rlimit::Resource::NOFILE.set(soft, hard)
    }
}

#[test]
fn test_ensure_max_open_files_limit() {
    fn other_error(msg: &str) -> std::io::Error {
        super::other_error(msg.to_string())
    }

    /// Mock implementation of NoFile interface.
    struct MockNoFile<'a>(&'a mut (u64, u64));

    impl<'a> NoFile for MockNoFile<'a> {
        fn get(&self) -> std::io::Result<(u64, u64)> {
            if self.0 .0 == 666 {
                Err(other_error("error"))
            } else {
                Ok(*self.0)
            }
        }

        fn set(&mut self, soft: u64, hard: u64) -> std::io::Result<()> {
            let (old_soft, old_hard) = self.get().unwrap();
            if old_hard == 666000 {
                Err(other_error("error"))
            } else {
                assert!(soft != old_soft, "Pointless call to set");
                *self.0 = (soft, hard);
                Ok(())
            }
        }
    }

    // We impose limit at i64::MAX and don’t even talk to the system if
    // number above that is requested.
    let msg = ensure_max_open_files_limit(MockNoFile(&mut (666, 666)), u64::MAX).unwrap_err();
    assert!(msg.contains("is too high"), "{msg}");

    // Error on get
    let msg = ensure_max_open_files_limit(MockNoFile(&mut (666, 666)), 1024).unwrap_err();
    assert!(msg.starts_with("Unable to get"), "{msg}");

    // Everything’s fine, soft limit should stay as it is.
    let mut state = (2048, 666000);
    ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap();
    assert_eq!((2048, 666000), state);

    // Should rise the soft limit.
    let mut state = (1024, 10000);
    ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap();
    assert_eq!((2024, 10000), state);

    // Should recognise trying to rise is futile because hard limit is too low.
    let mut state = (1024, 2000);
    let msg = ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap_err();
    assert!(msg.starts_with("Hard limit"), "{msg}");
    assert_eq!((1024, 2000), state);

    // Error trying to change the limit.
    let mut state = (1024, 666000);
    let msg = ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap_err();
    assert!(msg.starts_with("Unable to change"), "{msg}");
    assert_eq!((1024, 666000), state);
}
