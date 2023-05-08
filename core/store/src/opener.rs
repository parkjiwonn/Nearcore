use std::sync::Arc;
use strum::IntoEnumIterator;

use crate::db::rocksdb::snapshot::{Snapshot, SnapshotError, SnapshotRemoveError};
use crate::db::rocksdb::RocksDB;
use crate::metadata::{DbKind, DbMetadata, DbVersion, DB_VERSION};
use crate::{DBCol, DBTransaction, Mode, NodeStorage, Store, StoreConfig, Temperature};

#[derive(Debug, thiserror::Error)]
pub enum StoreOpenerError {
    /// I/O or RocksDB-level error while opening or accessing the database.
    #[error("{0}")]
    IO(#[from] std::io::Error),

    /// Database does not exist.
    ///
    /// This may happen when opening in ReadOnly or in ReadWriteExisting mode.
    #[error("Database does not exist")]
    DbDoesNotExist,

    /// Database already exists but requested creation of a new one.
    ///
    /// This may happen when opening in Create mode.
    #[error("Database already exists")]
    DbAlreadyExists,

    /// Hot database exists but cold doesn’t or the other way around.
    #[error("Hot and cold databases must either both exist or not")]
    HotColdExistenceMismatch,

    /// Hot and cold databases have different versions.
    #[error(
        "Hot database version ({hot_version}) doesn’t match \
         cold databases version ({cold_version})"
    )]
    HotColdVersionMismatch { hot_version: DbVersion, cold_version: DbVersion },

    /// Database has incorrect kind.
    ///
    /// Specifically, this happens if node is running with a single database and
    /// its kind is not RPC or Archive; or it’s running with two databases and
    /// their types aren’t Hot and Cold respectively.
    #[error("{which} database kind should be {want} but got {got:?}. Did you forget to set archive on your store opener?")]
    DbKindMismatch { which: &'static str, got: Option<DbKind>, want: DbKind },

    /// Unable to create a migration snapshot because one already exists.
    #[error(
        "Migration snapshot already exists at {0}; \
         unable to start new migration"
    )]
    SnapshotAlreadyExists(std::path::PathBuf),

    /// Creating the snapshot has failed.
    #[error("Error creating migration snapshot: {0}")]
    SnapshotError(std::io::Error),

    /// Deleting the snapshot after successful migration has failed.
    #[error("Error cleaning up migration snapshot: {error}")]
    SnapshotRemoveError {
        path: std::path::PathBuf,
        #[source]
        error: std::io::Error,
    },

    /// The database was opened for reading but it’s version wasn’t what we
    /// expect.
    ///
    /// This is an error because if the database is opened for reading we cannot
    /// perform database migrations.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         open in read-write mode (to run a migration) or use older neard"
    )]
    DbVersionMismatchOnRead { got: DbVersion, want: DbVersion },

    /// The database version isn’t what was expected and no migrator was
    /// configured.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         run node to perform migration or use older neard"
    )]
    DbVersionMismatch { got: DbVersion, want: DbVersion },

    /// The database version is missing.
    #[error("The database version is missing.")]
    DbVersionMissing {},

    /// Database has version which is no longer supported.
    ///
    /// `latest_release` gives latest neard release which still supports that
    /// database version.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         use neard {latest_release} to perform database migration"
    )]
    DbVersionTooOld { got: DbVersion, want: DbVersion, latest_release: &'static str },

    /// Database has version newer than what we support.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         update neard release"
    )]
    DbVersionTooNew { got: DbVersion, want: DbVersion },

    /// Error while performing migration.
    #[error("{0}")]
    MigrationError(#[source] anyhow::Error),
}

impl From<SnapshotError> for StoreOpenerError {
    fn from(err: SnapshotError) -> Self {
        match err {
            SnapshotError::AlreadyExists(snap_path) => Self::SnapshotAlreadyExists(snap_path),
            SnapshotError::IOError(err) => Self::SnapshotError(err),
        }
    }
}

impl From<SnapshotRemoveError> for StoreOpenerError {
    fn from(err: SnapshotRemoveError) -> Self {
        Self::SnapshotRemoveError { path: err.path, error: err.error }
    }
}

fn get_default_kind(archive: bool, temp: Temperature) -> DbKind {
    match (temp, archive) {
        (Temperature::Hot, false) => DbKind::RPC,
        (Temperature::Hot, true) => DbKind::Archive,
        (Temperature::Cold, _) => DbKind::Cold,
    }
}

fn is_valid_kind_temp(kind: DbKind, temp: Temperature) -> bool {
    match (kind, temp) {
        (DbKind::Cold, Temperature::Cold) => true,
        (DbKind::RPC, Temperature::Hot) => true,
        (DbKind::Hot, Temperature::Hot) => true,
        (DbKind::Archive, Temperature::Hot) => true,
        _ => false,
    }
}

fn is_valid_kind_archive(kind: DbKind, archive: bool) -> bool {
    match (kind, archive) {
        (DbKind::Archive, true) => true,
        (DbKind::Cold, true) => true,
        (DbKind::Hot, true) => true,
        (DbKind::RPC, _) => true,
        _ => false,
    }
}

/// Builder for opening node’s storage.
/// 노드의 저장소를 열기위한 빌더
/// Typical usage:
///
/// ```ignore
/// let store = NodeStorage::opener(&near_config.config.store)
///     .home(neard_home_dir)
///     .open();
/// ```
pub struct StoreOpener<'a> {
    /// Opener for an instance of RPC or Hot RocksDB store.
    /// RPC 또는 Hot RockDB 저장소의 인스턴스를 위한 따개
    hot: DBOpener<'a>,

    /// Opener for an instance of Cold RocksDB store if one was configured.
    /// cold RockDB 저장소가 구성된 경우 이 저장소의 인스턴스를 위한 따개
    cold: Option<DBOpener<'a>>,

    /// Whether the opener should expect archival db or not.
    /// 오프너가 아카이브 db를 기대해야하는지 아닌지
    /// 왜?
    archive: bool,

    /// A migrator which performs database migration if the database has old
    /// version.
    /// db의 이전 버전이 있다면 db 이주을 수행하는 이주자
    /// 이전버전에서 현재 버전으로 이주하는 것.
    migrator: Option<&'a dyn StoreMigrator>,
}

/// Opener for a single RocksDB instance.
/// 싱글 RocksDB 인스턴스의 오프너(따개)
struct DBOpener<'a> {
    /// Path to the database.
    ///
    /// This is resolved from nearcore home directory and store configuration
    /// passed to [`crate::NodeStorage::opener`].
    path: std::path::PathBuf,

    /// Configuration as provided by the user.
    config: &'a StoreConfig,

    /// Temperature of the database.
    ///
    /// This affects whether refcount merge operator is configured on reference
    /// counted column.  It’s important that the value is correct.  RPC and
    /// Archive databases are considered hot.
    temp: Temperature,
}

impl<'a> StoreOpener<'a> {
    /// Initialises a new opener with given home directory and store config.
    /// 주어진 홈 디렉토리와 저장소 conifg가 있는 새로운 오프너를 초기화한다.
    pub(crate) fn new(
        home_dir: &std::path::Path, /// 홈 디렉토리
        archive: bool, /// 아카이브 db를 기대해야하는지 아닌지
        config: &'a StoreConfig, /// 저장소 구성
        cold_config: Option<&'a StoreConfig>, /// cold DB 구성
    ) -> Self {
        Self { /// StoreOpener 구조체를 초기화함.
            hot: DBOpener::new(home_dir, config, Temperature::Hot), /// hot은 DBOpener 구조체로 인스턴스를 RPC 또는 Hot RocksDB 저장소에 대한 오프너로 설정함.
            cold: cold_config.map(|config| DBOpener::new(home_dir, config, Temperature::Cold)), /// Cold RocksDB 저장소에 대한 오프너 설정
            archive: archive, /// 아카이브 DB 여부를 나타냄
            migrator: None, /// StoreMigrator 트레이트를 구현하는 객체
        }
    }

    /// Configures the opener with specified [`StoreMigrator`].
    /// 명시된 StoreMigrator로 오프너를 구성하다.
    /// If the migrator is not configured, the opener will fail to open databases with older versions.
    /// 만약 migrator가 구성되지 않았다면 opener는 이전 버전의 데이터 베이스를 여는데 실패할 것이다.
    /// With migrator configured, it will attempt to perform migrations.
    /// 구성된 migrator와 migrations을 수행하기를 시도할 것 이다.
    pub fn with_migrator(mut self, migrator: &'a dyn StoreMigrator) -> Self {
        /// StoreOpener에 대해 StoreMigrator 트레이트 객체 사용해서 migration 수행하도록 지시함.
        self.migrator = Some(migrator);
        /// StoreOpener 구조체 생성할때 migrator가 none 이었는데
        /// Some(migrator) 해줌으로써 StoreMigrator 트레이트 객체를 설정해준것.
        /// StoreMigrator는 storage의 버전을 확인하고 버전 이전을 해야할 경우 버전 이전을 해주는 것.
        self
    }

    /// Returns path to the underlying RocksDB database.
    ///
    /// Does not check whether the database actually exists.
    pub fn path(&self) -> &std::path::Path {
        &self.hot.path
    }

    #[cfg(test)]
    pub(crate) fn config(&self) -> &StoreConfig {
        self.hot.config
    }

    /// Opens the storage in read-write mode.
    /// 읽고-쓰는 모드로 저장소를 열어라
    /// Creates the database if missing.
    /// 만약 잃어버릴 경우 데이터 베이스를 생성해라
    pub fn open(&self) -> Result<crate::NodeStorage, StoreOpenerError> {
        self.open_in_mode(Mode::ReadWrite)/// 모드는 읽고 쓰기
        /// 읽기/쓰기 모드로 DB 열고 NodeStorage 반환한다.
    }

    /// Opens the RocksDB database(s) for hot and cold (if configured) storages.
    /// hot 그리고 cold(구성된 경우) 저장소에 대한 RocksDB 데이터 베이스를 연다.
    /// When opening in read-only mode, verifies that the database version is
    /// what the node expects and fails if it isn’t.
    /// 오직 읽기 모드에서 열었을 때, db 버전이 노드가 예상하는 것과 일치하는지 확인하고 그렇지 않은 경우 실패한다.
    /// If database doesn’t exist,
    /// creates a new one unless mode is [`Mode::ReadWriteExisting`].
    /// 만약 db가 존재하지 않는다면 모드가 [`Mode::ReadWriteExisting`] (이미 존재하면 실패하기때문) 가 아닌 이상 새로운 것을 만든다.
    /// On the other hand, if mode is [`Mode::Create`], fails if the database already
    /// exists.
    /// 다른 말로 만약 모드가 [`Mode::Create`]인 경우 만약 db가 이미 존재한다면 실패한다.
    /// readwriteexisting은 존재하면 실패 create는 존재하지 않으면 다시 만듬. 존재하면 만들 이유가 없음.
    /// open_in_mode는 매개변수로 mode를 받고 nodestorage를 반환한다.
    /// -> 읽기 쓰기 모드로 존재하는 db를 연다. 만약 존재하지 않으면 다시 만든다.
    pub fn open_in_mode(&self, mode: Mode) -> Result<crate::NodeStorage, StoreOpenerError> {
        /// open_in_mode: StoreOpener 구조체의 메서드
        /// StoreOpener와 mode를 매개변수로 받고 NodeStorage를 반환한다.
        {
            /// hot_path & cold_path 경로를 출력한다.
            /// cold_path는 경로가 있는 경우에 출력한다.
            let hot_path = self.hot.path.display().to_string();
            let cold_path = match &self.cold {
                Some(cold) => cold.path.display().to_string(),
                None => String::from("none"),
            };
            /// node 저장소를 열었다.
            tracing::info!(target: "db_opener", path=hot_path, cold_path=cold_path, "Opening NodeStorage");
        }

        let hot_snapshot = {
            /// hot 디렉토리에 대한 스냅샷 생성한다.
            Self::ensure_created(mode, &self.hot)?;
            Self::ensure_kind(mode, &self.hot, self.archive, Temperature::Hot)?;
            Self::ensure_version(mode, &self.hot, &self.migrator)?
        };

        let cold_snapshot = if let Some(cold) = &self.cold {
            /// cold 디렉토리에 대한 스냅샷 생성한다.
            /// 스냅샷 : 특정 시점의 시스템 정보를 저장한다.
            Self::ensure_created(mode, cold)?;
            Self::ensure_kind(mode, cold, self.archive, Temperature::Cold)?;
            Self::ensure_version(mode, cold, &self.migrator)?
        } else {
            Snapshot::none()
        };

        let (hot_db, _) = self.hot.open(mode, DB_VERSION)?;
        let cold_db = self
            .cold
            .as_ref()
            .map(|cold| cold.open(mode, DB_VERSION))
            .transpose()?
            .map(|(db, _)| db);

        let storage = NodeStorage::from_rocksdb(hot_db, cold_db);

        hot_snapshot.remove()?;
        cold_snapshot.remove()?;

        Ok(storage)
    }

    pub fn create_snapshots(&self, mode: Mode) -> Result<(Snapshot, Snapshot), StoreOpenerError> {
        {
            let hot_path = self.hot.path.display().to_string();
            let cold_path = match &self.cold {
                Some(cold) => cold.path.display().to_string(),
                None => String::from("none"),
            };
            tracing::info!(target: "db_opener", path=hot_path, cold_path=cold_path, "Creating NodeStorage snapshots");
        }

        let hot_snapshot = {
            Self::ensure_created(mode, &self.hot)?;
            Self::ensure_kind(mode, &self.hot, self.archive, Temperature::Hot)?;
            let snapshot = Self::ensure_version(mode, &self.hot, &self.migrator)?;
            if snapshot.0.is_none() {
                self.hot.snapshot()?
            } else {
                snapshot
            }
        };

        let cold_snapshot = if let Some(cold) = &self.cold {
            Self::ensure_created(mode, cold)?;
            Self::ensure_kind(mode, cold, self.archive, Temperature::Cold)?;
            let snapshot = Self::ensure_version(mode, cold, &self.migrator)?;
            if snapshot.0.is_none() {
                cold.snapshot()?
            } else {
                snapshot
            }
        } else {
            Snapshot::none()
        };

        Ok((hot_snapshot, cold_snapshot))
    }

    // Creates the DB if it doesn't exist.
    /// 만약 db가 존재하지 않는다면 db를 생성한다.
    fn ensure_created(mode: Mode, opener: &DBOpener) -> Result<(), StoreOpenerError> {
        let meta = opener.get_metadata()?; /// db의 메타데이터를 가져옴.
        /// 메타데이터가 존재하는 경우 db가 이미 존재하는 것으로 간주됨.
        /// 메타데이터? 다른 데이터를 설명해주는 데이터이다.
        /// 예를 들어 디지털 카메라에서 사진 찍어서 기록할때 사진 자체의 정보와 시간,장소 부가적인 데이터를 같이 저장한다.
        /// 이런 부가적인 데이터를 분석해서 이용하면 사진을 적절하게 정리하거나 다시 가공할때 다시 유용하게 쓸 수 있는 정보가 된다.
        /// 그래서 데이터에 대한 데이터라고 하는 것.
        match meta {
            /// must_create 는 create mode 존재하면 실해하는 것. 읽기 쓰기 가능
            ///
            Some(_) if !mode.must_create() => {
                /// false -> DB가 존재한다.
                tracing::info!(target: "db_opener", path=%opener.path.display(), "The database exists.");
                return Ok(());
            }
            Some(_) => {
                return Err(StoreOpenerError::DbAlreadyExists);
            }
            None if mode.can_create() => {
                tracing::info!(target: "db_opener", path=%opener.path.display(), "The database doesn't exist, creating it.");

                let db = opener.create()?;
                let store = Store { storage: Arc::new(db) };
                store.set_db_version(DB_VERSION)?;
                return Ok(());
            }
            None => {
                return Err(StoreOpenerError::DbDoesNotExist);
            }
        }
    }

    /// Ensures that the db has correct kind. If the db doesn't have kind
    /// it sets it, if the mode allows, or returns an error.
    /// db에 올바른 종류가 있는지 확인함. DB에 종류가 없는 경우 모드가 허용하는 경우 종류를 설정하거나 오류를 반환함.??
    /// => RocksDB 가 올바른 종류(kind)인지 확인하는 메서드임.
    /// db 종류 확인하고 종류가 지정되어 있찌 않은 경우에 설저함.
    /// hot 인지 cold 인지 확인하는 것.
    fn ensure_kind(
        mode: Mode,
        opener: &DBOpener,
        archive: bool,
        temp: Temperature,
    ) -> Result<(), StoreOpenerError> {
        let which: &'static str = temp.into();
        tracing::debug!(target: "db_opener", path = %opener.path.display(), archive, which, "Ensure db kind is correct and set.");
        let store = Self::open_store_unsafe(mode, opener)?;

        let current_kind = store.get_db_kind()?;
        let default_kind = get_default_kind(archive, temp);
        let err =
            Err(StoreOpenerError::DbKindMismatch { which, got: current_kind, want: default_kind });

        // If kind is set check if it's the expected one.
        if let Some(current_kind) = current_kind {
            if !is_valid_kind_temp(current_kind, temp) {
                return err;
            }
            if !is_valid_kind_archive(current_kind, archive) {
                return err;
            }
            return Ok(());
        }

        // Kind is not set, set it.
        /// 종류가 결정 안되어 있으면 설정해라.
        if mode.read_write() {
            tracing::info!(target: "db_opener", archive,  which, "Setting the db DbKind to {default_kind:#?}");

            store.set_db_kind(default_kind)?;
            /// db 종류 설정하는 메서드
            return Ok(());
        }

        return err;
    }

    /// Ensures that the db has the correct - most recent - version.
    /// db가 맞는 가장 최근의 버전을 갖고있는지 확인한다.
    /// If the version is lower, it performs migrations up until the most recent version, if mode allows or returns an error.
    /// 만약 버전이 낮다면 가장 최근의 버전까지 db 이주를 시킨다.
    fn ensure_version(
        mode: Mode,
        opener: &DBOpener,
        migrator: &Option<&dyn StoreMigrator>,
    ) -> Result<Snapshot, StoreOpenerError> {
        tracing::debug!(target: "db_opener", path=%opener.path.display(), "Ensure db version");

        let metadata = opener.get_metadata()?;
        let metadata = metadata.ok_or(StoreOpenerError::DbDoesNotExist {})?;
        let DbMetadata { version, .. } = metadata;

        if version == DB_VERSION {
            return Ok(Snapshot::none());
        }
        if version > DB_VERSION {
            return Err(StoreOpenerError::DbVersionTooNew { got: version, want: DB_VERSION });
        }

        // If we’re opening for reading, we cannot perform migrations thus we
        // must fail if the database has old version (even if we support
        // migration from that version).
        /// 읽기 모드에서는 migration 수행할 수 X
        /// 오래된 버전은 그냥 읽기 밖에 못하나?
        /// db를 읽기만 한다며는 당연히 수정이 안되겠지 편집모드가 아니니까
        /// 따라서 오래된 버전이 있다면 실패할 것이다
        /// 읽기 모드 = 오래된 버전으로 해석하면 되나
        if mode.read_only() {
            return Err(StoreOpenerError::DbVersionMismatchOnRead {
                got: version,
                want: DB_VERSION,
            });
        }

        // Figure out if we have migrator which supports the database version.
        /// db migration 지원하는 migrator를 가지고 있는지 확인한다.
        let migrator = migrator
            .ok_or(StoreOpenerError::DbVersionMismatch { got: version, want: DB_VERSION })?;
        if let Err(release) = migrator.check_support(version) {
            return Err(StoreOpenerError::DbVersionTooOld {
                got: version,
                want: DB_VERSION,
                latest_release: release,
            });
        }

        let snapshot = opener.snapshot()?;

        for version in version..DB_VERSION {
            tracing::info!(target: "db_opener", path=%opener.path.display(),
                           "Migrating the database from version {} to {}",
                           version, version + 1);

            // Note: here we open the cold store as a regular Store object
            // backed by RocksDB. It doesn't matter today as we don't expect any
            // old migrations on the cold storage. In the future however it may
            // be better to wrap it in the ColdDB object instead.

            let store = Self::open_store(mode, opener, version)?;
            migrator.migrate(&store, version).map_err(StoreOpenerError::MigrationError)?;
            store.set_db_version(version + 1)?;
        }

        if cfg!(feature = "nightly") || cfg!(feature = "nightly_protocol") {
            let version = 10000;
            tracing::info!(target: "db_opener", path=%opener.path.display(),
            "Setting the database version to {version} for nightly");

            // Set some dummy value to avoid conflict with other migrations from
            // nightly features.
            let store = Self::open_store(mode, opener, DB_VERSION)?;
            store.set_db_version(version)?;
        }

        Ok(snapshot)
    }

    fn open_store(
        mode: Mode,
        opener: &DBOpener,
        version: DbVersion,
    ) -> Result<Store, StoreOpenerError> {
        let (db, _) = opener.open(mode, version)?;
        let store = Store { storage: Arc::new(db) };
        Ok(store)
    }

    fn open_store_unsafe(mode: Mode, opener: &DBOpener) -> Result<Store, StoreOpenerError> {
        let db = opener.open_unsafe(mode)?;
        let store = Store { storage: Arc::new(db) };
        Ok(store)
    }
}

impl<'a> DBOpener<'a> {
    /// Constructs new opener for a single RocksDB builder.
    ///
    /// The path to the database is resolved based on the path in config with
    /// given home_dir as base directory for resolving relative paths.
    fn new(home_dir: &std::path::Path, config: &'a StoreConfig, temp: Temperature) -> Self {
        let path = if temp == Temperature::Hot { "data" } else { "cold-data" };
        let path = config.path.as_deref().unwrap_or(std::path::Path::new(path));
        let path = home_dir.join(path);
        Self { path, config, temp }
    }

    /// Returns version and kind of the database or `None` if it doesn’t exist.
    ///
    /// If the database exists but doesn’t have version set, returns an error.
    /// Similarly if the version key is set but to value which cannot be parsed.
    ///
    /// For database versions older than the point at which database kind was
    /// introduced, the kind is returned as `None`.  Otherwise, it’s also
    /// fetched and if it’s not there error is returned.
    fn get_metadata(&self) -> std::io::Result<Option<DbMetadata>> {
        RocksDB::get_metadata(&self.path, self.config)
    }

    /// Opens the database in given mode checking expected version and kind.
    ///
    /// Fails if the database doesn’t have version given in `want_version`
    /// argument.  Note that the verification is meant as sanity checks.
    /// Verification failure either indicates an internal logic error (since
    /// caller is expected to know the version) or some strange file system
    /// manipulations.
    ///
    /// The proper usage of this method is to first get the metadata of the
    /// database and then open it knowing expected version and kind.  Getting
    /// the metadata is a safe operation which doesn’t modify the database.
    /// This convoluted (one might argue) process is therefore designed to avoid
    /// modifying the database if we’re opening something with a too old or too
    /// new version.
    ///
    /// Use [`Self::create`] to create a new database.
    fn open(&self, mode: Mode, want_version: DbVersion) -> std::io::Result<(RocksDB, DbMetadata)> {
        let db = RocksDB::open(&self.path, &self.config, mode, self.temp)?;
        let metadata = DbMetadata::read(&db)?;
        if want_version != metadata.version {
            let msg = format!("unexpected DbVersion {}; expected {want_version}", metadata.version);
            Err(std::io::Error::new(std::io::ErrorKind::Other, msg))
        } else {
            Ok((db, metadata))
        }
    }

    /// Opens the database in given mode without checking the expected version and kind.
    ///
    /// This is only suitable when creating the database or setting the version
    /// and kind for the first time.
    fn open_unsafe(&self, mode: Mode) -> std::io::Result<RocksDB> {
        let db = RocksDB::open(&self.path, &self.config, mode, self.temp)?;
        Ok(db)
    }

    /// Creates a new database.
    fn create(&self) -> std::io::Result<RocksDB> {
        RocksDB::open(&self.path, &self.config, Mode::Create, self.temp)
    }

    /// Creates a new snapshot for the database.
    fn snapshot(&self) -> Result<Snapshot, SnapshotError> {
        Snapshot::new(&self.path, &self.config, self.temp)
    }
}

pub trait StoreMigrator {
    /// Checks whether migrator supports database versions starting at given.
    /// migrator가 지정된 버전 부터 db 버전을 지원하는지 여부를 확인함.
    /// If the `version` is too old and the migrator no longer supports it,
    /// returns `Err` with the latest neard release which supported that
    /// version.  Otherwise returns `Ok(())` indicating that the migrator
    /// supports migrating the database from the given version up to the current
    /// version [`DB_VERSION`].
    /// '버전'이 너무 오래되어 migrator에서 더이상 지원하지 않는 경우, 해당 버전을 지원하는 최신 니어 릴리스와 함께 'Err'를 반환한다.
    /// 그렇지 않으면 OK(())를 반환해서 migrator가 지정된 버전에서 현재 버전 ['DB_VERSION']으로 db migration을 지원함을 나타냅니다.
    /// **Panics** if `version` ≥ [`DB_VERSION`].
    fn check_support(&self, version: DbVersion) -> Result<(), &'static str>;

    /// Performs database migration from given version to the next one.
    /// 지정된 버전에서 다음 버전으로 db migration을 수행한다.
    /// The function only does single migration from `version` to `version + 1`.
    /// 이 기능은 version에서 version+1 로의 단일 migration만 수행한다.
    /// It doesn’t update database’s metadata (i.e. what version is stored in
    /// the database) which is responsibility of the caller.
    /// 호출자가 담당하는 db의 메타 데이터(즉, db에 저장된 버전) 를 업데이트하지 않는다.
    /// **Panics** if `version` is not supported (the caller is supposed to
    /// check support via [`Self::check_support`] method) or if it’s greater or
    /// equal to [`DB_VERSION`].
    /// version이 지원되지 않는 경우 또는 더 큰 경우 ['DB_VERISON']과 같다.
    fn migrate(&self, store: &Store, version: DbVersion) -> anyhow::Result<()>;
}

/// Creates checkpoint of hot storage in `home_dir.join(checkpoint_relative_path)`
///
/// If `columns_to_keep` is None doesn't cleanup columns.
/// Otherwise deletes all columns that are not in `columns_to_keep`.
///
/// Returns NodeStorage of checkpoint db.
/// `archive` -- is hot storage archival (needed to open checkpoint).
#[allow(dead_code)]
pub fn checkpoint_hot_storage_and_cleanup_columns(
    db_storage: &NodeStorage,
    home_dir: &std::path::Path,
    checkpoint_relative_path: std::path::PathBuf,
    columns_to_keep: Option<Vec<DBCol>>,
    archive: bool,
) -> anyhow::Result<NodeStorage> {
    let checkpoint_path = home_dir.join(checkpoint_relative_path);

    db_storage.hot_storage.create_checkpoint(&checkpoint_path)?;

    // As only path from config is used in StoreOpener, default config with custom path will do.
    let mut config = StoreConfig::default();
    config.path = Some(checkpoint_path);
    let opener = StoreOpener::new(home_dir, archive, &config, None);
    let node_storage = opener.open()?;

    if let Some(columns_to_keep) = columns_to_keep {
        let columns_to_keep_set: std::collections::HashSet<DBCol> =
            std::collections::HashSet::from_iter(columns_to_keep.into_iter());
        let mut transaction = DBTransaction::new();

        for col in DBCol::iter() {
            if !columns_to_keep_set.contains(&col) {
                transaction.delete_all(col);
            }
        }

        node_storage.hot_storage.write(transaction)?;
    }

    Ok(node_storage)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_keys_existence(store: &Store, column: &DBCol, keys: &Vec<Vec<u8>>, expected: bool) {
        for key in keys {
            assert_eq!(store.exists(*column, &key).unwrap(), expected, "Column {:?}", column);
        }
    }

    #[test]
    fn test_checkpoint_hot_storage_and_cleanup_columns() {
        let (home_dir, opener) = NodeStorage::test_opener();
        let node_storage = opener.open().unwrap();

        let keys = vec![vec![0], vec![1], vec![2], vec![3]];
        let columns = vec![DBCol::Block, DBCol::Chunks, DBCol::BlockHeader];

        let mut store_update = node_storage.get_hot_store().store_update();
        for column in columns {
            for key in &keys {
                store_update.insert(column, key, &vec![42]);
            }
        }
        store_update.commit().unwrap();

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &node_storage,
            &home_dir.path(),
            std::path::PathBuf::from("checkpoint_none"),
            None,
            false,
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, true);

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &node_storage,
            &home_dir.path(),
            std::path::PathBuf::from("checkpoint_some"),
            Some(vec![DBCol::Block]),
            false,
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, false);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, false);

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &node_storage,
            &home_dir.path(),
            std::path::PathBuf::from("checkpoint_all"),
            Some(vec![DBCol::Block, DBCol::Chunks, DBCol::BlockHeader]),
            false,
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, true);

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &node_storage,
            &home_dir.path(),
            std::path::PathBuf::from("checkpoint_empty"),
            Some(vec![]),
            false,
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, false);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, false);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, false);
    }
}
