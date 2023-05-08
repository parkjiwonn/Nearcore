mod cli;// cli 모듈 가져옴 -> cli.rs 파일을 모듈로 가져옴. 노드 커맨드라인 인터페이스 구현한 코드 들어있음.

use self::cli::NeardCmd;// cli 모듈 내에 있는 NeardCmd 구조체 가져옴. 이 구조체는 near 노드의 명령어를 처리하는 로직 담고 있음.
use anyhow::Context;// anyhow crate에 있는 Context trait 가져옴
// 이거 사용하면 error messgae에서 additional context 제공할 수 있음.
// Result 타입의 에러 메시지에 추가적인 문맥정보(context)를 제공하는 기능 제공함. Context trait를 사용하면 Result 타입에서 에러가 발생했을 때, 해당 에러에 대한 추가적인 정보를 제공할 수 있습니다.
use near_primitives::version::{Version, PROTOCOL_VERSION};//near_primitives crate에서 Version과 PROTOCOL_VERSION을 가져옵니다.
// Nearprotocol의 기본 데이터 구조 제공 / Version : 니어 프로토콜 버전을 담고 있는 구조체, Protocol_version : 니어 프로토콜 버전 나타내는 상수임.
use near_store::metadata::DB_VERSION;// near_store crate에서 DB_VERSION을 가져옵니다.
// near db를 관리하는 데 필요한 모듈 제공함.
use nearcore::get_default_home;// nearcore 모듈 내의 get_default_home 함수를 가져옵니다.
use once_cell::sync::Lazy;// once_cell crate에서 Lazy를 가져옵니다. !!!Lazy를 사용하면 값을 한번만 계산하고 이후로는 재사용합니다.!!!
// 상수 값을 미리 계산해 놓고 재사용하도록 하기 위해 사용됨.
// 왜 재사용하는 가? -> Lazy 사용하면 값의 계산 비용이 큰 경우에 한번만 계산하고 이후에는 메모리에 저장된 값을 재사용해 성능을 향상시킬 수 있다.
use std::env;//std crate에서 env 모듈을 가져옵니다. 이 모듈은 환경변수를 다룰 수 있도록 합니다.
use std::path::PathBuf; // std crate에서 path 모듈의 PathBuf 타입을 가져옵니다.
// Pathbuf : 파일 시스템 경로를 저장할 수 있는 가변적인 타입
use std::time::Duration; // std crate에서 time 모듈의 Duration 타입을 가져옵니다.
// rust에서 시간을 다루는 데 필요한 모듈임. 스케줄러 생성해서 출력 주기 설정하는데 사용됨.

// 환경변수 선언하는 부분 : NEARD_VERSION / NEARD_BUILD / RUSTC_VERSION
static NEARD_VERSION: &str = env!("NEARD_VERSION"); // 상수 NEARD_VERSION을 선언합니다. 이 값을 가져오기 위해서는 NEARD_VERSION이라는 환경변수가 필요합니다.
static NEARD_BUILD: &str = env!("NEARD_BUILD"); // 상수 NEARD_BUILD를 선언합니다. 이 값을 가져오기 위해서는 NEARD_BUILD라는 환경변수가 필요합니다.
static RUSTC_VERSION: &str = env!("NEARD_RUSTC_VERSION"); // 상수 RUSTC_VERSION을 선언합니다. 이 값을 가져오기 위해서는 NEARD_RUSTC_VERSION이라는 환경변수가 필요합니다.

//상수 NEARD_VERSION_STRING을 선언합니다.
//이 값을 Lazy로 선언하고, 람다식 내부에서 NEARD_VERSION, NEARD_BUILD, RUSTC_VERSION, PROTOCOL_VERSION, DB_VERSION 값을 이용하여 String을 만듭니다.
static NEARD_VERSION_STRING: Lazy<String> = Lazy::new(|| {
    // Lazy로 선언했기 때문에 노드가 실행될 때마다 생성되지 X , 한번 생성된 문자열이 재사용되어 성능을 향상시킴.
    format!(
        "(release {}) (build {}) (rustc {}) (protocol {}) (db {})",
        NEARD_VERSION, NEARD_BUILD, RUSTC_VERSION, PROTOCOL_VERSION, DB_VERSION
    )
}); // neard --version 명령어를 실행할 때 출력되는 정보 중 하나?

// neard_version 함수를 정의
//이 함수는 NEARD_VERSION, NEARD_BUILD, RUSTC_VERSION 값을 이용하여 Version 구조체를 생성하여 반환합니다.
fn neard_version() -> Version {
    Version {
        version: NEARD_VERSION.to_string(),
        build: NEARD_BUILD.to_string(),
        rustc_version: RUSTC_VERSION.to_string(),
    }
}

// 상수 DEFAULT_HOME을 선언합니다. 이 값을 Lazy로 선언하고, get_default_home 함수를 이용하여 PathBuf를 생성합니다.
static DEFAULT_HOME: Lazy<PathBuf> = Lazy::new(get_default_home);
// 정적 변수 선언 -> Lazy 사용해 get_default_home 함수를 한 번만 실행하고 이후에는 저장된 값을 재사용할 수 있도록 함.
// 변수를 사용할 때마다 함수를 호출하지 않아도 되서 성능 향상할 수 있음.
// get_default_home 설명은 타고 들어가면 주석에 있음.


// global_allocator attribute를 이용하여 ALLOC 상수를 선언합니다. tikv_jemallocator crate의 Jemalloc(전역 할당자)을 사용합니다.
// jemalloc 전역 할당자를 프로그램의 기본 할당자로 사용하도록 지정하는데 사용됨. / 효율적인 메모리 할당 및 해제를 위한 고성능의 메모리 할당기
#[global_allocator] // 속성 : 프로그램 전체에 적용될 수 있도록 지정한다. / 해당 전역 할당자를 기본 할당자로 지정할 수 있음.
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc; // 프로그램의 모든 메모리 할당과 해제가 Jemalloc 의 구현을 사용하도록 보장함.
// 매우 큰 메모리 할당 작업이나 많은 수의 메모리 할당 작업이 필요한 경우에 jemalloc 같은 외부 allocator를 사용해 효율적으로 메모리 할당하기 위함.
// 그리고 메모리 누수 검사해서 디버그할 때 도움 될 수 있음.

// 메인 함수 부분
fn main() -> anyhow::Result<()> {
    //anyhow는 Rust 언어에서 오류 처리(error handling)를 단순화하고 개선하기 위한 라이브러리입니다.
    //anyhow는 Result 타입을 반환하는 함수에서 오류 처리를 쉽게 하도록 해줌.
    if env::var("RUST_BACKTRACE").is_err() {
        // env::var 는 해당 환경 변수의 값을 반환하거나 값이 없으면 Err를 반환.
        // .is_err()는 Err인지 아닌지 검사하는 메서드
        // 따라서 RUST_BACKTRACE 환경변수가 설정되어 있지 않으면 env::set_var을 이용해 RUST_BACKTRACE 활성화함.
        // Enable backtraces on panics by default.(기본적으로 패닉에 대한 백트레이스를 활성화 한다.)
        env::set_var("RUST_BACKTRACE", "1");
        // RUST_BACKTRACE 환경 변수를 "1"로 설정하여 프로그램에서 패닉이 발생하면 백트레이스 정보를 출력하도록 합니다.
        // 백트레이스 : 프로그램 실행 중에 발생한 오류에 대한 디버깅 정보를 제공한다. 패닉을 일으킨 위치를 추적할 수 있음.
        // 패닉을 일으킨 위치 & 이전 함수 호출 스택 정보 출력함.
        // 백트레이스는 런타임 오류가 발생한 시점에서 디버깅 정보 제공하는 것.
    }

    // Rayon 라이브러리에서 사용하는 스레드풀을 생성합니다. 이때 각 스레드의 스택 크기를 8MB로 설정합니다. 그리고 생성된 스레드풀을 전역 스코프에 등록합니다.
    // 전역 스레드 풀을 만드는 이유 : 여러 곳에서 병렬 작업을 수행해야 할 때가 있음. 그때마다 새로운 스레드 풀을 만들면 작업에 부담이 가게됨.
    // 그래서 전역에서 사용할 수있는 스레드 풀을 만드는 것.
    // rayon : 순차 연산을 병렬로 쉽게 변환할 수 있는 데이터 병렬 처리 라이브러리, 기존 코드에 병렬 처리를 도입하는데 가볍고 편리
    // threadpool ? 병렬로 함수들을 실행하기 위해 사용되어지는 것.
    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)// 각 스레드가 사용할 스택의 크기 설졍
        .build_global()// 전역 스레드 풀 만듬.
        .context("failed to create the threadpool")?; // 해당 문구의 컨텍스트 정보와 함께 anyhow::Error 타입의 에러를 반환함.
    // context 함수는 Result 타입에 대해 에러 메시지에 추가적인 컨텍스트를 제공해 ? 연산자를 이용해 Result를 반환함.

    // We use it to automatically search the for root certificates to perform HTTPS calls
    // = 루트 인증서를 자동으로 검색해 HTTPS 호출을 수행하는 데 사용함.
    // (sending telemetry and downloading genesis)
    // = 원격 측정 전송 및 제네시스 다운로드 등의 작업을 수행할 때 필요함.
    // OpenSSL 라이브러리에서 HTTPS 호출을 할 때 필요한 root certificates를 자동으로 검색할 수 있도록 환경 변수를 초기화
    // 왜 HTTPS 호출을 할 때 root 인증서를 설정해야하는건지? 인증서 발급기관 (CA)의 root 인증서가 필요함.
    // root 인증서는 인증서 검증 시 사용되고 이 인증서가 신뢰할 수 있는 CA에서 서명된 것이면 그 인증서가 신뢰할 수 있는 인증서라고 판단할 수 있음.
    openssl_probe::init_ssl_cert_env_vars();
    // NEAR Protocol에서 사용하는 performance metrics를 출력하는 스케줄러를 생성합니다. 60초마다 출력합니다.
    // perfomance metrics 성능 측정 지표 -> 60초 마다 성능 측정 지표를 출력함.
    // 성능을 모니터링할 수 있음.
    near_performance_metrics::process::schedule_printing_performance_stats(Duration::from_secs(60));

    // The default FD soft limit in linux is 1024.(리눅스의 기본 FD 소프트 제한은 1024)
    // We use more than that, for example we support up to 1000 TCP (기본 보다 더 사용한다 예를 들어 각 연결당 5 FD를 사용해 최대 1000 TCP연결을 지원함.
    // connections, using 5 FDs per each connection.
    // We consider 65535 to be a reasonable limit for this binary, (이 바이너리에 대해서 65535를 합리적인 제한으로 간주함.)
    // and we enforce it here. We also set the hard limit to the same value(여기서 이를 적용함. 하드 리밋도 같은 값으로 설정함)
    // to prevent the inner logic from trying to bump it further: ( 내부 로직이 더이상 충동하지 않도록 방지함)
    // FD limit is a global variable, so it shouldn't be modified in an ( FD 리밋이 전역 변수여서 조정되지 않은 방식으로 수정해서는 안됨)
    // uncoordinated way.
    // FD ? 리눅스는 각 프로세스마다 파일 디스크립터를 할당, FD는 파일이나 소켓 등의 리소스를 가리키는 역할을 하는 정수값?
    // 자세한 설명 : 리눅스는 모든 리소스가 파일로 처리된다. 그래서 이런 파일들에 접근하기 위해서 FD를 사용한다. 특정 파일에 접근하기 위한 추상적인 값.
    // 그래서 각 계정마다 프로세스가 사용할 수 있는 FD의 갯수가 정해져있다.
    // soft limit : 새로운 프로세스가 생성될 경우 기본적으로 적용되는 값
    // hard limit : soft limit 부터 늘릴 수 있는 최대 값
    const FD_LIMIT: u64 = 65535;// 파일 디스크립터(filed descriptor)의 소프트 제한 값과 하드 제한 값을 모두 65535로 설정합니다.
    let (_, hard) = rlimit::Resource::NOFILE.get().context("rlimit::Resource::NOFILE::get()")?;
    //현재 하드 제한 값을 가져옵니다.
    rlimit::Resource::NOFILE.set(FD_LIMIT, FD_LIMIT).context(format!(
        "couldn't set the file descriptor limit to {FD_LIMIT}, hard limit = {hard}"
    ))?;

    NeardCmd::parse_and_run()
}
