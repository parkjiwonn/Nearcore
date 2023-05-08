pub use crate::config::{init_configs, load_config, load_test_config, NearConfig, NEAR_BASE};
pub use crate::runtime::NightshadeRuntime;

use crate::cold_storage::spawn_cold_store_loop;
use crate::state_sync::{spawn_state_sync_dump, StateSyncDumpHandle};
use actix::{Actor, Addr};
use actix_rt::ArbiterHandle;
use anyhow::Context;
use cold_storage::ColdStoreLoopHandle;
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::messaging::{IntoSender, LateBoundSender};
use near_async::time;
use near_chain::{Chain, ChainGenesis};
use near_chunks::shards_manager_actor::start_shards_manager;
use near_client::{start_client, start_view_client, ClientActor, ConfigUpdater, ViewClientActor};
use near_network::PeerManagerActor;
use near_primitives::block::GenesisId;
use near_store::metadata::DbKind;
use near_store::metrics::spawn_db_metrics_loop;
use near_store::{DBCol, Mode, NodeStorage, Store, StoreOpenerError};
use near_telemetry::TelemetryActor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

pub mod append_only_map;
mod cold_storage;
pub mod config;
mod config_validate;
mod download_file;
pub mod dyn_config;
mod metrics;
pub mod migrations;
mod runtime;
mod state_sync;

// near의 기본 홈 디렉토리를 구하는 함수
pub fn get_default_home() -> PathBuf {
    if let Ok(near_home) = std::env::var("NEAR_HOME") {
        return near_home.into();
    }

    if let Some(mut home) = dirs::home_dir() {
        home.push(".near");
        return home;
    }

    PathBuf::default()
}

/// Opens node’s storage performing migrations and checks when necessary.
/// 채굴을 수행하는 노드의 저장소를 열고 필요한 경우 확인한다.
/// If opened storage is an RPC store and `near_config.config.archive` is true,
/// converts the storage to archival node.
/// 만약 열린 저장소가 RPC 저장소이고 near_config.config.archive 가 true인 경우 저장소를 아카이브로 변환한다.
/// Otherwise, if opening archival node
/// with that field being false, prints a warning and sets the field to `true`.
/// 그렇지 않으면 만약 필드가 거짓인 아카이브 모드를 열면 경고를 출력하고 필드를 true로 설정한다.
/// In other words, once store is archival, the node will act as archival nod
/// regardless of settings in `config.json`.
/// 다른말로 일단 저장소가 아카이브 노드로 전환되면 config.json 설정에 관계없이 노드는 아카이브 노드로 동작한다.
/// The end goal is to get rid of `archive` option in `config.json` file and
/// have the type of the node be determined purely based on kind of database
/// being opened.
/// 최종 목표는 config.json 파일에서 아카이브 옵션을 없애고 노드 유형이 열리는 db의 종류에 따라 순전히노드 유형이 결정되도록 하는 것.
/// ( 정리 )
/// 노드의 유형이 열리는 스토리지 종류에 따라 노드 유형이 결정되도록 하는 것이다.
fn open_storage(home_dir: &Path, near_config: &mut NearConfig) -> anyhow::Result<NodeStorage> {/// anyhow : 코드에서 발생하는 에러를 한가지 타입으로 처리할수있게 도와주는 라이브러리
    let migrator = migrations::Migrator::new(near_config); /// Migrator 구조체 생성함.
    /// migrator : 노드 스토리지의 버전을 변경시켜주는 것.
    let opener = NodeStorage::opener( /// NodeStorage의 opener 메서드
        home_dir,
        near_config.client_config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    )
    .with_migrator(&migrator);
    /// match 표현식 사용해서 open() 메서드 호출 결과를 처리함.
    /// 호출 결과에 따라 다양한 분기를 수행함.
    let storage = match opener.open() {
        /// 읽기 쓰기 모드로 hot cold 저장소에 대한 RocksDB 열고 난 다음 상황들을 분기처리한것.
        Ok(storage) => Ok(storage),
        Err(StoreOpenerError::IO(err)) => {
            Err(anyhow::anyhow!("{err}"))
        }
        // Cannot happen with Mode::ReadWrite
        Err(StoreOpenerError::DbDoesNotExist) => unreachable!(),
        // Cannot happen with Mode::ReadWrite
        Err(StoreOpenerError::DbAlreadyExists) => unreachable!(),
        Err(StoreOpenerError::HotColdExistenceMismatch) => {
            Err(anyhow::anyhow!(
                "Hot and cold databases must either both exist or both not exist.\n\
                 Note that at this moment it’s not possible to convert and RPC or legacy archive database into split hot+cold database.\n\
                 To set up node in that configuration, start with neither of the databases existing.",
            ))
        },
        Err(err @ StoreOpenerError::HotColdVersionMismatch { .. }) => {
            Err(anyhow::anyhow!("{err}"))
        },
        Err(StoreOpenerError::DbKindMismatch { which, got, want }) => {
            Err(if let Some(got) = got {
                anyhow::anyhow!("{which} database kind should be {want} but got {got}")
            } else {
                anyhow::anyhow!("{which} database kind should be {want} but none was set")
            })
        }
        Err(StoreOpenerError::SnapshotAlreadyExists(snap_path)) => {
            Err(anyhow::anyhow!(
                "Detected an existing database migration snapshot at ‘{}’.\n\
                 Probably a database migration got interrupted and your database is corrupted.\n\
                 Please replace files in ‘{}’ with contents of the snapshot, delete the snapshot and try again.",
                snap_path.display(),
                opener.path().display(),
            ))
        },
        Err(StoreOpenerError::SnapshotError(err)) => {
            use near_store::config::MigrationSnapshot;
            let path = std::path::PathBuf::from("/path/to/snapshot/dir");
            let on = MigrationSnapshot::Path(path).format_example();
            let off = MigrationSnapshot::Enabled(false).format_example();
            Err(anyhow::anyhow!(
                "Failed to create a database migration snapshot: {err}.\n\
                 To change the location of snapshot adjust \
                 ‘store.migration_snapshot’ property in ‘config.json’:\n{on}\n\
                 Alternatively, you can disable database migration snapshots \
                 in `config.json`:\n{off}"
            ))
        },
        Err(StoreOpenerError::SnapshotRemoveError { path, error }) => {
            let path = path.display();
            Err(anyhow::anyhow!(
                "The DB migration has succeeded but deleting of the snapshot \
                 at {path} has failed: {error}\n
                 Try renaming the snapshot directory to temporary name (e.g. \
                 by adding tilde to its name) and starting the node.  If that \
                 works, the snapshot can be deleted."))
        }
        // Cannot happen with Mode::ReadWrite
        Err(StoreOpenerError::DbVersionMismatchOnRead { .. }) => unreachable!(),
        // Cannot happen when migrator is specified.
        Err(StoreOpenerError::DbVersionMismatch { .. }) => unreachable!(),
        Err(StoreOpenerError::DbVersionMissing { .. }) => {
            Err(anyhow::anyhow!("Database version is missing!"))
        },
        Err(StoreOpenerError::DbVersionTooOld { got, latest_release, .. }) => {
            Err(anyhow::anyhow!(
                "Database version {got} is created by an old version \
                 of neard and is no longer supported, please migrate using \
                 {latest_release} release"
            ))
        },
        Err(StoreOpenerError::DbVersionTooNew { got, want }) => {
            Err(anyhow::anyhow!(
                "Database version {got} is higher than the expected version {want}. \
                It was likely created by newer version of neard. Please upgrade your neard."
            ))
        },
        Err(StoreOpenerError::MigrationError(err)) => {
            Err(err)
        },
    }.with_context(|| format!("unable to open database at {}", opener.path().display()))?;

    near_config.config.archive = storage.is_archive()?;
    /// db에서 메타데이터 확인하고 저장소가 아카이브인지 아닌지 확인하고 아카이브 boolean값 설정한다.
    /// 왜냐하면 storage 확인하고 노드의 유형을 결정한다고 했으니까.
    Ok(storage)
}

// Safely get the split store while checking that all conditions to use it are met.
fn get_split_store(config: &NearConfig, storage: &NodeStorage) -> anyhow::Result<Option<Store>> {
    // SplitStore should only be used on archival nodes.
    if !config.config.archive {
        return Ok(None);
    }

    // SplitStore should only be used if cold store is configured.
    if config.config.cold_store.is_none() {
        return Ok(None);
    }

    // SplitStore should only be used in the view client if it is enabled.
    if !config.config.split_storage.as_ref().map_or(false, |c| c.enable_split_storage_view_client) {
        return Ok(None);
    }

    // SplitStore should only be used if the migration is finished. The
    // migration to cold store is finished when the db kind of the hot store is
    // changed from Archive to Hot.
    if storage.get_hot_store().get_db_kind()? != Some(DbKind::Hot) {
        return Ok(None);
    }

    Ok(storage.get_split_store())
}

/// Node 구조체
pub struct NearNode {
    pub client: Addr<ClientActor>,
    pub view_client: Addr<ViewClientActor>,
    pub arbiters: Vec<ArbiterHandle>, // 중재자 - 검증자를 말하는 건가?
    pub rpc_servers: Vec<(&'static str, actix_web::dev::ServerHandle)>,
    /// The cold_store_loop_handle will only be set if the cold store is configured.
    /// It's a handle to a background thread that copies data from the hot store to the cold store.+
    /// 콜드 스토어가 구성된 경우에만 콜드 스토어 루프 핸들이 설정됩니다.
    /// 핫 스토어에서 콜드 스토어로 데이터를 복사하는 백그라운드 스레드에 대한 핸들입니다.
    /// 콜드 스토어 = 콜드 wallet(오프라인 지갑), 핫 스토어 = 핫 wallet(online based wallet)?
    /// 콜드는 안전하게 데이터 보관&엑세스 제한, 핫은 엑세스가 가능, 데이터에 빠른 접근이 필요할때 사용
    /// 근데 핫에서 콜드로 데이터 복사할 때 백에서 스레드가 도는 거지
    pub cold_store_loop_handle: Option<ColdStoreLoopHandle>,
    /// Contains handles to background threads that may be dumping state to S3.
    /// 상태를 S3에 덤프할 수 있는 백그라운드 스레드에 대한 핸들을 포함합니다.
    /// S3에 왜 덤프할까? 백업&복원 같은 목적 때문에
    /// S3? 아마존 웹 서비스에서 제공하는 클라우드 스토리지 서비스
    pub state_sync_dump_handle: Option<StateSyncDumpHandle>,
    /// StateSyncDumpHandle 는 생성된 스레드의 수명을 제어하는 arbiter 핸들을 보유함.
    ///
}

/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!실제 진입점!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
pub fn start_with_config(home_dir: &Path, config: NearConfig) -> anyhow::Result<NearNode> {
    /// 인수 : home_dir(홈 디렉토리 경로), config(nearconfig 구조체)
    /// NearNode라는 결과 타입 반환함.
    start_with_config_and_synchronization(home_dir, config, None, None)
}
/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!실제 진입점!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

pub fn start_with_config_and_synchronization(
    home_dir: &Path,
    mut config: NearConfig,
    // 'shutdown_signal' will notify the corresponding `oneshot::Receiver` when an instance of
    // `ClientActor` gets dropped.
    shutdown_signal: Option<broadcast::Sender<()>>,
    config_updater: Option<ConfigUpdater>,
) -> anyhow::Result<NearNode> {
    let storage = open_storage(home_dir, &mut config)?;
    /// open_storage 함수 사용해서 스토리지 열고, config 구조체 업데이트함.
    ///
    let db_metrics_arbiter = if config.client_config.enable_statistics_export {
        let period = config.client_config.log_summary_period;
        /// metrics 정보 수집해야하는 주기 설정
        let db_metrics_arbiter_handle = spawn_db_metrics_loop(&storage, period)?;
        Some(db_metrics_arbiter_handle)
    } else {
        None
    };

    let runtime = NightshadeRuntime::from_config(home_dir, storage.get_hot_store(), &config);

    // Get the split store. If split store is some then create a new runtime for
    // the view client. Otherwise just re-use the existing runtime.
    let split_store = get_split_store(&config, &storage)?;
    let view_runtime = if let Some(split_store) = split_store {
        NightshadeRuntime::from_config(home_dir, split_store, &config)
    } else {
        runtime.clone()
    };

    let cold_store_loop_handle = spawn_cold_store_loop(&config, &storage, runtime.clone())?;

    let telemetry = TelemetryActor::new(config.telemetry_config.clone()).start();
    let chain_genesis = ChainGenesis::new(&config.genesis);
    let genesis_block = Chain::make_genesis_block(&*runtime, &chain_genesis)?;
    let genesis_id = GenesisId {
        chain_id: config.client_config.chain_id.clone(),
        hash: *genesis_block.header().hash(),
    };

    let node_id = config.network_config.node_id();
    let network_adapter = Arc::new(LateBoundSender::default());
    let shards_manager_adapter = Arc::new(LateBoundSender::default());
    let client_adapter_for_shards_manager = Arc::new(LateBoundSender::default());
    let adv = near_client::adversarial::Controls::new(config.client_config.archive);

    let view_client = start_view_client(
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        chain_genesis.clone(),
        view_runtime,
        network_adapter.clone().into(),
        config.client_config.clone(),
        adv.clone(),
    );
    let (client_actor, client_arbiter_handle) = start_client(
        config.client_config.clone(),
        chain_genesis.clone(),
        runtime.clone(),
        node_id,
        network_adapter.clone().into(),
        shards_manager_adapter.as_sender(),
        config.validator_signer.clone(),
        telemetry,
        shutdown_signal,
        adv,
        config_updater,
    );
    client_adapter_for_shards_manager.bind(client_actor.clone().with_auto_span_context());
    let (shards_manager_actor, shards_manager_arbiter_handle) = start_shards_manager(
        runtime.clone(),
        network_adapter.as_sender(),
        client_adapter_for_shards_manager.as_sender(),
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        storage.get_hot_store(),
        config.client_config.chunk_request_retry_period,
    );
    shards_manager_adapter.bind(shards_manager_actor);

    let state_sync_dump_handle = spawn_state_sync_dump(&config, chain_genesis, runtime)?;

    #[allow(unused_mut)]
    let mut rpc_servers = Vec::new();
    let network_actor = PeerManagerActor::spawn(
        time::Clock::real(),
        storage.into_inner(near_store::Temperature::Hot),
        config.network_config,
        Arc::new(near_client::adapter::Adapter::new(client_actor.clone(), view_client.clone())),
        shards_manager_adapter.as_sender(),
        genesis_id,
    )
    .context("PeerManager::spawn()")?;
    network_adapter.bind(network_actor.clone().with_auto_span_context());

    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        rpc_servers.extend(near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone(),
            view_client.clone(),
            Some(network_actor),
        ));
    }

    #[cfg(feature = "rosetta_rpc")]
    if let Some(rosetta_rpc_config) = config.rosetta_rpc_config {
        rpc_servers.push((
            "Rosetta RPC",
            near_rosetta_rpc::start_rosetta_rpc(
                rosetta_rpc_config,
                config.genesis,
                genesis_block.header().hash(),
                client_actor.clone(),
                view_client.clone(),
            ),
        ));
    }

    rpc_servers.shrink_to_fit();

    tracing::trace!(target: "diagnostic", key = "log", "Starting NEAR node with diagnostic activated");

    let mut arbiters = vec![client_arbiter_handle, shards_manager_arbiter_handle];
    if let Some(db_metrics_arbiter) = db_metrics_arbiter {
        arbiters.push(db_metrics_arbiter);
    }

    Ok(NearNode {
        client: client_actor,
        view_client,
        rpc_servers,
        arbiters,
        cold_store_loop_handle,
        state_sync_dump_handle,
    })
}

pub struct RecompressOpts {
    pub dest_dir: PathBuf,
    pub keep_partial_chunks: bool,
    pub keep_invalid_chunks: bool,
    pub keep_trie_changes: bool,
}

pub fn recompress_storage(home_dir: &Path, opts: RecompressOpts) -> anyhow::Result<()> {
    use strum::IntoEnumIterator;

    let config_path = home_dir.join(config::CONFIG_FILENAME);
    let config = config::Config::from_file(&config_path)
        .map_err(|err| anyhow::anyhow!("{}: {}", config_path.display(), err))?;
    let archive = config.archive;
    let mut skip_columns = Vec::new();
    if archive && !opts.keep_partial_chunks {
        skip_columns.push(DBCol::PartialChunks);
    }
    if archive && !opts.keep_invalid_chunks {
        skip_columns.push(DBCol::InvalidChunks);
    }
    if archive && !opts.keep_trie_changes {
        skip_columns.push(DBCol::TrieChanges);
    }

    let src_opener = NodeStorage::opener(home_dir, archive, &config.store, None);
    let src_path = src_opener.path();

    let mut dst_config = config.store.clone();
    dst_config.path = Some(opts.dest_dir);
    // Note: opts.dest_dir is resolved relative to current working directory
    // (since it’s a command line option) which is why we set home to cwd.
    let cwd = std::env::current_dir()?;
    let dst_opener = NodeStorage::opener(&cwd, archive, &dst_config, None);
    let dst_path = dst_opener.path();

    info!(target: "recompress",
          src = %src_path.display(), dest = %dst_path.display(),
          "Recompressing database");

    info!("Opening database at {}", src_path.display());
    let src_store = src_opener.open_in_mode(Mode::ReadOnly)?.get_hot_store();

    let final_head_height = if skip_columns.contains(&DBCol::PartialChunks) {
        let tip: Option<near_primitives::block::Tip> =
            src_store.get_ser(DBCol::BlockMisc, near_store::FINAL_HEAD_KEY)?;
        anyhow::ensure!(
            tip.is_some(),
            "{}: missing {}; is this a freshly set up node? note that recompress_storage makes no sense on those",
            src_path.display(),
            std::str::from_utf8(near_store::FINAL_HEAD_KEY).unwrap(),
        );
        tip.map(|tip| tip.height)
    } else {
        None
    };

    info!("Creating database at {}", dst_path.display());
    let dst_store = dst_opener.open_in_mode(Mode::Create)?.get_hot_store();

    const BATCH_SIZE_BYTES: u64 = 150_000_000;

    for column in DBCol::iter() {
        let skip = skip_columns.contains(&column);
        info!(
            target: "recompress",
            column_id = column as usize,
            %column,
            "{}",
            if skip { "Clearing  " } else { "Processing" }
        );
        if skip {
            continue;
        }

        let mut store_update = dst_store.store_update();
        let mut total_written: u64 = 0;
        let mut batch_written: u64 = 0;
        let mut count_keys: u64 = 0;
        for item in src_store.iter_raw_bytes(column) {
            let (key, value) = item.with_context(|| format!("scanning column {column}"))?;
            store_update.set_raw_bytes(column, &key, &value);
            total_written += value.len() as u64;
            batch_written += value.len() as u64;
            count_keys += 1;
            if batch_written >= BATCH_SIZE_BYTES {
                store_update.commit()?;
                info!(
                    target: "recompress",
                    column_id = column as usize,
                    %count_keys,
                    %total_written,
                    "Processing",
                );
                batch_written = 0;
                store_update = dst_store.store_update();
            }
        }
        info!(
            target: "recompress",
            column_id = column as usize,
            %count_keys,
            %total_written,
            "Done with "
        );
        store_update.commit()?;
    }

    // If we’re not keeping DBCol::PartialChunks, update chunk tail to point to
    // current final block.  If we don’t do that, the gc will try to work its
    // way from the genesis even though chunks at those heights have been
    // deleted.
    if skip_columns.contains(&DBCol::PartialChunks) {
        let chunk_tail = final_head_height.unwrap();
        info!(target: "recompress", %chunk_tail, "Setting chunk tail");
        let mut store_update = dst_store.store_update();
        store_update.set_ser(DBCol::BlockMisc, near_store::CHUNK_TAIL_KEY, &chunk_tail)?;
        store_update.commit()?;
    }

    core::mem::drop(dst_store);
    core::mem::drop(src_store);

    info!(target: "recompress", dest = %dst_path.display(), "Database recompressed");
    Ok(())
}
