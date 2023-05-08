#[cfg(unix)]
use anyhow::Context;
use near_amend_genesis::AmendGenesisCommand;
use near_chain_configs::GenesisValidationMode;
use near_client::ConfigUpdater;
use near_cold_store_tool::ColdStoreCommand;
use near_dyn_configs::{UpdateableConfigLoader, UpdateableConfigLoaderError, UpdateableConfigs};
use near_flat_storage::commands::FlatStorageCommand;
use near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse;
use near_mirror::MirrorCommand;
use near_network::tcp;
use near_o11y::tracing_subscribㅁer::EnvFilter;
use near_o11y::{
    default_subscriber, default_subscriber_with_opentelemetry, BuildEnvFilterError,
    EnvFilterBuilder,
};
use near_ping::PingCommand;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::compute_root_from_path;
use near_primitives::types::{Gas, NumSeats, NumShards};
use near_state_parts::cli::StatePartsCommand;
use near_state_viewer::StateViewerSubCommand;
use near_store::db::RocksDB;
use near_store::Mode;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tracing::{debug, error, info, warn};
use near_o11y::tracing_subscriber::EnvFilter;

/// NEAR Protocol Node
#[derive(clap::Parser)]
// clap 라이브러리의 Parser trait을 구현하기 위한 attribute.
// clap은 cli 구성하기 위한 라이브러리
// 서브커맨드 구현가능
// 서브커맨드? cli에서 사용되는 명령어 하위 집합
#[clap(version = crate::NEARD_VERSION_STRING.as_str())]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(super) struct NeardCmd {
    #[clap(flatten)]
    opts: NeardOpts, // Neafr 노드 옵션 정의하는 구조체
    #[clap(subcommand)]
    subcmd: NeardSubCommand, // 노드의 서브 커맨드 정의하는 열거형
}
/// NearCmd : Near 노드 실행에 필요한 인수들을 저장하고 있는 구조체

impl NeardCmd {
    /// parse_and_run : cli 명령어 파싱하고 해당 명령어에 따라 노드 실행하는 역할 함.
    pub(super) fn parse_and_run() -> anyhow::Result<()> {

        let neard_cmd: Self = clap::Parser::parse();
        /// Self -> NeardCmd 구조체 생성 , cli 명령어 파싱해서 neard_cmd 변수에 할당하는 코드

        // Enable logging of the current thread. (현재 스레드의 로깅 활성화 ??)
        let _subscriber_guard = default_subscriber(
            make_env_filter(neard_cmd.opts.verbose_target())?,
            &neard_cmd.opts.o11y,
        )
            .local();

        /// info! : rust의 로깅 매크로 중 하나, 코드 실행 중에 발생하는 이벤트를 기록할 때 사용됨.
        ///
        info!(
            target: "neard",
            version = crate::NEARD_VERSION,
            build = crate::NEARD_BUILD,
            latest_protocol = near_primitives::version::PROTOCOL_VERSION
        ); /// version, build, protocol version 출력
        /// 노드 버전, 빌드 정보, 프로토콜 현재 버전 출력하는 로그

        #[cfg(feature = "test_features")]
        /// test_features 기능이 활성화된 경우에 컴파일 된다.
        /// test_features는 많은 cargo.toml 파일을 타고타고 들어가야함.
        {
            error!("THIS IS A NODE COMPILED WITH ADVERSARIAL BEHAVIORS. DO NOT USE IN PRODUCTION.");
            /// 이 노드는 적대적인 동작으로 컴파일된 노드입니다. 프로덕션 환경에서 사용하지 마세요.
            /// 왜 프로덕션 환경? 실제 운영환경에서 사용하지 말아야 하는 악성 노드가 컴파일 됐다는 것을 알리기 위함.
            if std::env::var("ADVERSARY_CONSENT").unwrap_or_default() != "1" {
                /// ADVERSARY_CONSENT 환경변수, 악의적인 행동을 수행하는 노드를 실행하기 전에 사용자의 동의를 확인하기 위해 사용됨.
                /// 환경 변수값 =1 이면 악의적인 행동을 수행하는 노드를 실행하는데 동의한 것.
                error!(
                    "To run a node with adversarial behavior enabled give your consent \
                            by setting an environment variable:"
                    /// 적대적 동작이 활성화된 노드를 실행하려면 환경 변수를 설정해 동의한다.
                );
                error!("ADVERSARY_CONSENT=1");
                std::process::exit(1);
            }
        } /// 악의적인 행동을 수행하는 노드를 실행하는데 동의하는지 확인하기 위해 사용됨. 1이 아닌경우 즉 동의하지 않은 경우
        /// 노드 실행을 중단하고 오류메세지 출력함.

        /// Near 노드가 데이터를 저장할 디렉토리 경로를 저장함.
        let home_dir = neard_cmd.opts.home.clone();
        /// 블체 최초 상태를 검증할 때 사용하는 검증 모드 선택하는 변수
        let genesis_validation = if neard_cmd.opts.unsafe_fast_startup {
            /// unsafe_fast_startup 이 true 일때
            GenesisValidationMode::UnsafeFast
            /// 블록체인 상태를 검증하는 데 드는 시간을 단축시키지만 검증이 덜 엄격해 보안성 위험
        } else {
            /// unsafe_fast_startup 이 false 일때
            GenesisValidationMode::Full
            /// 블체 상태 검증하는데 시간 더 걸리지만 보안성 위험 줄어듬
        };
        /// 정리 : Near 노드가 데이터를 저장할 디렉토리 경로 & 블록체인의 최초 상태를 검증하는데 사용할 검증 모드를 설정.
        /// 블록체인의 최초 상태를 검증? genesis block은 블체의 초기 상태를 정의함.

        /// neard_cmd 변수의 subcmd 필드를 기반으로 실행할 서브 커맨드를 선택해 run 메서드 실행하는 코드
        match neard_cmd.subcmd {
            /// 쉽게 - neardcmd.subcmd 변수에 따라서 서브 명령 실행함.
            ///
            /// near 프로토콜 초기화 구현
            /// = NEAR 노드 초기화 하고 실행 준비 마친다.
            NeardSubCommand::Init(cmd) => cmd.run(&home_dir)?,

            NeardSubCommand::Localnet(cmd) => cmd.run(&home_dir),

            NeardSubCommand::Run(cmd) => cmd.run(
                &home_dir,
                genesis_validation,
                neard_cmd.opts.verbose_target(),
                &neard_cmd.opts.o11y,
            ),

            NeardSubCommand::StateViewer(cmd) => {
                let mode = if cmd.readwrite { Mode::ReadWrite } else { Mode::ReadOnly };
                cmd.subcmd.run(&home_dir, genesis_validation, mode, cmd.store_temperature);
            }

            NeardSubCommand::RecompressStorage(cmd) => {
                cmd.run(&home_dir);
            }
            NeardSubCommand::VerifyProof(cmd) => {
                cmd.run();
            }
            NeardSubCommand::Ping(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::Mirror(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::AmendGenesis(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::ColdStore(cmd) => {
                cmd.run(&home_dir)?;
            }
            NeardSubCommand::StateParts(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::FlatStorage(cmd) => {
                cmd.run(&home_dir)?;
            }
            NeardSubCommand::ValidateConfig(cmd) => {
                cmd.run(&home_dir)?;
            }
        };
        Ok(())
    }
}

#[derive(clap::Parser)]
pub(super) struct StateViewerCommand {
    /// By default state viewer opens rocks DB in the read only mode, which allows it to run
    /// multiple instances in parallel and be sure that no unintended changes get written to the DB.
    /// In case an operation needs to write to caches, a read-write mode may be needed.
    #[clap(long, short = 'w')]
    readwrite: bool,
    /// What store temperature should the state viewer open. Allowed values are hot and cold but
    /// cold is only available when cold_store is configured.
    /// Cold temperature actually means the split store will be used.
    #[clap(long, short = 't', default_value = "hot")]
    store_temperature: near_store::Temperature,
    #[clap(subcommand)]
    subcmd: StateViewerSubCommand,
}

#[derive(clap::Parser, Debug)]
struct NeardOpts {
    /// Sets verbose logging for the given target, or for all targets if no
    /// target is given.
    #[clap(long, name = "target")]
    verbose: Option<Option<String>>,
    /// Directory for config and data.
    #[clap(long, parse(from_os_str), default_value_os = crate::DEFAULT_HOME.as_os_str())]
    home: PathBuf,
    /// Skips consistency checks of genesis.json (and records.json) upon startup.
    /// Let's you start `neard` slightly faster.
    #[clap(long)]
    unsafe_fast_startup: bool,
    /// Enables export of span data using opentelemetry protocol.
    #[clap(flatten)]
    o11y: near_o11y::Options,
}

impl NeardOpts {
    pub fn verbose_target(&self) -> Option<&str> {
        match self.verbose {
            None => None,
            Some(None) => Some(""),
            Some(Some(ref target)) => Some(target.as_str()),
        }
    }
}

#[derive(clap::Parser)]
pub(super) enum NeardSubCommand {
    /// Initializes NEAR configuration ( 노드 구성 초기화 )
    Init(InitCmd),
    /// Runs NEAR node ( 노드 실행 )
    Run(RunCmd),
    /// 모든 필요한 파일들로 로컬 구성 설정하는 것.
    /// Sets up local configuration with all necessary files (validator key, node key, genesis and
    /// config)
    Localnet(LocalnetCmd),
    /// View DB state.
    /// DB 상태를 보는 것.
    #[clap(name = "view-state", alias = "view_state")]
    StateViewer(StateViewerCommand),
    /// Recompresses the entire storage.  This is a slow operation which reads
    /// all the data from the database and writes them down to a new copy of the
    /// database.
    /// Db의 모든 데이터 읽고 새 복사본에 기록하는 느린 작업임.
    /// In 1.26 release the compression algorithm for the database has changed
    /// to reduce storage size.  Nodes don’t need to do anything for new data to
    /// take advantage of better compression but existing data may take months
    /// to be recompressed.  This may be an issue for archival nodes which keep
    /// hold of all the old data.
    /// 압축 알고리즘 변경 -> 스토리지 크기 줄임. 근데 기존 데이터를 재압축하는데 몇달이 더 걸릴수있음.
    /// 그래서 이전 데이터 모두 보관하는 아카이브 노드에 문제가 될 수 있음.
    /// This command makes it possible to force the recompression as a one-time
    /// operation.  Using it reduces the database even by up to 40% though that
    /// is partially due to database ‘defragmentation’ (whose effects will wear
    /// off in time).  Still, reduction by about 20% even if that’s taken into
    /// account can be expected.
    /// 근데 이 명령어를 사용하면 재압축을 일화성작업으로 강제할 수 있음.
    /// db 최대 40%까지 줄일 수 있음.
    /// It’s important to remember however, that this command may take up to
    /// a day to finish in which time the database cannot be used by the node.
    /// 이 작업은 최대 하루 정도 걸리고 노드는 db 사용할 수 없음.
    /// Furthermore, file system where output directory is located needs enough
    /// free space to store the new copy of the database.  It will be smaller
    /// than the original but to be safe one should provision around the same
    /// space as the size of the current `data` directory.
    /// 그리고 새 복사본을 저장할 수 있는 여유로운 공간이 필요해 원본보다는 작겠지 근데 안전을 위해서 db의 크기와 동일한 공간을 확보해야돼
    /// Finally, because this command is meant only as a temporary migration
    /// 그래서 이 명령은 임시 마이그레이션 도구로만 사용돼.
    /// 임시 마이그레이션 도구 ? near 프로토콜의 업그레이드 과정에서 사용됩니다.
    /// tool, it is planned to be removed by the end of 2022.
    /// 2022년 말에 제거될 예정? 지금 제거된건가?
    #[clap(alias = "recompress_storage")]
    RecompressStorage(RecompressStorageSubCommand),

    /// Verify proofs
    /// 증명 확인
    #[clap(alias = "verify_proof")]
    VerifyProof(VerifyProofSubCommand),

    /// Connects to a NEAR node and sends ping messages to the accounts it sends
    /// us after the handshake is completed, printing stats to stdout.
    /// near 노드에 연결하고 핸드쉐이크 과정이 완료된 후에 해당 노드로부터 전송되는 게정들에게 ping 메시지를 보내는 프로그램을 실행하는 것을 의미
    /// 프로그램은 ping 메세지 전송의 결과를 콘솔에 출력합니다.
    /// ping 메세지 ? 노드 간의 연결상태와 성능을 검증하기 위해 사용되는 메시지 (gpt)
    Ping(PingCommand),

    /// Mirror transactions from a source chain to a test chain with state forked
    /// from it, reproducing traffic and state as closely as possible.
    /// 트래픽 & 상태를 최대한 가깝게 재현하면서 트랜잭션을 소스 체인에서 상태가 분기된 테스트 체인으로 미러링(복제?)한다.
    /// 블체에서 발생하는 트랜잭션 & 상태를 테스트하는 과정에서 사용될 수 있음.
    /// 원래 체인에서 발생하는 상황을 최대한 유사하게 모방해 테스트 체인에서 발생할 수 있는 문제점이나 버그를 사전에 발견할 수 있도록 함.
    /// 이게 왜 필요한 걸까.. 테스트용 블체 구성하고 테스트를 수행하는 데 필요한 명령어와 도구 제공하기 위함?
    Mirror(MirrorCommand),

    /// Amend a genesis/records file created by `dump-state`.
    /// 덤프 상태로 만들어진 제네시스/기록 파일을 수정한다.
    /// 덤프 상태 ? 블록체인의 상태를 스냅샷 형태로 저장하는 것 의미 함. 덤프 파일로 저장하면 블체 상태 빠르게 복원&백업할 수 있음.
    AmendGenesis(AmendGenesisCommand),

    /// Testing tool for cold storage
    /// cold storage ? 오프라인에서 개인키를 안전하게 보관하는것.
    ColdStore(ColdStoreCommand),

    /// Connects to a NEAR node and sends state parts requests after the handshake is completed.
    /// 핸드 쉐이크가 완료된 후에 상태 부품 요청을 보내고 near 노드에 연결한다.
    /// 상태 데이터를 동기화하는 과정인가?
    /// 핸드 쉐이크 - 노드간의 통신을 설정하는데 사용, 통신 시작하기 전에 두 개체간의 정보를 교환하는 과정
    StateParts(StatePartsCommand),

    /// Flat storage related tooling.
    /// 왜 flat 이란 단어를 썼을까?
    FlatStorage(FlatStorageCommand),

    /// validate config files including genesis.json and config.json
    /// genesis.json & config.json을 포함한 config 파일들을 검증한다.
    ValidateConfig(ValidateConfigCommand),
}

/// near 프로토콜의 초기화 커맨드를 구현한 구조체
#[derive(clap::Parser)]
pub(super) struct InitCmd {
    /// Download the verified NEAR genesis file automatically.
    /// 검증된 near genesis 파일을 자동으로 다운로드
    #[clap(long)]
    download_genesis: bool,
    /// Download the verified NEAR config file automatically.
    #[clap(long)]
    download_config: bool,
    /// Makes block production fast (TESTING ONLY).
    /// 블록 생성을 빠르게 만듬 (테스트 용으로만)
    #[clap(long)]
    fast: bool,
    /// Account ID for the validator key.
    /// 검증자 키를 위한 계정 id
    #[clap(long)]
    account_id: Option<String>,
    /// Chain ID, by default creates new random.
    /// 체인 id, 기본적으로 새로운 random 값 생성함.
    #[clap(long, forbid_empty_values = true)]
    chain_id: Option<String>,
    /// Specify a custom download URL for the genesis file.
    /// genesis 파일을 위한 커스텀 다운로드 url을 명시하다.
    #[clap(long)]
    download_genesis_url: Option<String>,
    /// Specify a custom download URL for the records file.
    /// records 파일을 위해 커스텀 다운로드 url을 명시하다.
    #[clap(long)]
    download_records_url: Option<String>,
    /// Specify a custom download URL for the config file.
    /// config 파일을 위해 커스텀 다운로드 url을 명시하다.
    #[clap(long)]
    download_config_url: Option<String>,
    /// Genesis file to use when initializing testnet (including downloading).
    /// 테스트넷 초기화에 사용할 genesis 파일
    #[clap(long)]
    genesis: Option<String>,
    /// Initialize boots nodes in <node_key>@<ip_addr> format seperated by commas
    /// to bootstrap the network and store them in config.json(= 노드가 실행될 때 사용되는 구성 파일, 네트워크에 대한 정보 포함)
    /// 부팅 노드를 쉼표로 구분된 <node_key>@<ip_addr> 형식으로 초기화화여 네트워크를 부트스트랩하고 config.json에 저장함.
    /// 부트스트램? 새로운 노드가 네트워크에 참여할 때 초기 설정을 받아오는 과정. 노드가 처음 시작될 때 다른 노드들과 통신해 초기 설정 파일 & 정보를 받아오는 것을 의미함.
    #[clap(long)]
    boot_nodes: Option<String>,
    /// Number of shards to initialize the chain with.
    /// chain에 초기화할 샤드의 수 지정, 기본값은 1
    #[clap(long, default_value = "1")]
    num_shards: NumShards,
    /// Specify private key generated from seed (TESTING ONLY).
    /// seed에 생성된 프라이빗 키 생성(테스트 용도)
    /// seed는 난수 생성기에서 생성된 무작위 문자열 -> 개인키 & 공개키 생성 가능
    #[clap(long)]
    test_seed: Option<String>,
    /// Customize max_gas_burnt_view runtime limit.  If not specified, value
    /// from genesis configuration will be taken.
    /// 최대 가스 버닝 러타임 제한을 커스텀함. 만약 지정하지 않으면 제네시스 설정 값을 사용한다.
    #[clap(long)]
    max_gas_burnt_view: Option<Gas>,
}

/// Warns if unsupported build of the executable is used on mainnet or testnet.
/// 메인넷 또는 테스트넷에서 지원되지 않는 실행 파일 빌드가 사용되는 경우 경고함.
/// Verifies that when running on mainnet or testnet chain a neard binary built
/// 메인넷 또는 테스트넷 체인에서 실행할 때 'make release' 명령으로 빌드한 neard binary가 사용되었는지 확인함.
/// with `make release` command is used.  That Makefile targets enable
/// 해당 makefile 타겟이 다른 방법으로 빌드할 때 활성화되지 않는 최적화 옵션을 활성화하는지 확인함.
/// optimisation options which aren’t enabled when building with different
/// 최적화 옵션을 활성화하며, 공식적으로 지원되는 유일한 바이너리 빌드 방법임.
/// methods and is the only officially supported method of building the binary
/// to run in production.
///
/// The detection is done by checking that `NEAR_RELEASE_BUILD` environment
/// 감지는 NEAR_RELEASE_BUILD 환경 변수가 release로 설정되어 있는지 확인하여 수행한다.
/// variable was set to `release` during compilation (which is what Makefile
/// 그리고 nightly or nightly_protocol 기능이 활성화되어 있지 않습니다.
/// sets) and that neither `nightly` nor `nightly_protocol` features are
/// enabled.
fn check_release_build(chain: &str) {
    // 현재 실행 파일이 release build인지 확인하고 그렇지 않은 경우 경고를 발생시키는것.
    let is_release_build = option_env!("NEAR_RELEASE_BUILD") == Some("release")
        && !cfg!(feature = "nightly")
        && !cfg!(feature = "nightly_protocol");
    if !is_release_build && ["mainnet", "testnet"].contains(&chain) {
        warn!(
            target: "neard",
            "Running a neard executable which wasn’t built with `make release` \
             command isn’t supported on {}.",
            chain
        );
        warn!(
            target: "neard",
            "Note that `cargo build --release` builds lack optimisations which \
             may be needed to run properly on {}",
            chain
        );
        warn!(
            target: "neard",
            "Consider recompiling the binary using `make release` command.");
    }
}

impl InitCmd {
    pub(super) fn run(self, home_dir: &Path) -> anyhow::Result<()> {
        // TODO: Check if `home` exists. If exists check what networks we already have there.
        if (self.download_genesis || self.download_genesis_url.is_some()) && self.genesis.is_some()
        {
            anyhow::bail!("Please give either --genesis or --download-genesis, not both.");
        }

        if let Some(chain) = self.chain_id.as_ref() {
            check_release_build(chain)
        }

        nearcore::init_configs(
            home_dir,
            self.chain_id,
            self.account_id.and_then(|account_id| account_id.parse().ok()),
            self.test_seed.as_deref(),
            self.num_shards,
            self.fast,
            self.genesis.as_deref(),
            self.download_genesis,
            self.download_genesis_url.as_deref(),
            self.download_records_url.as_deref(),
            self.download_config,
            self.download_config_url.as_deref(),
            self.boot_nodes.as_deref(),
            self.max_gas_burnt_view,
        )
            .context("Failed to initialize configs")
    }
}

#[derive(clap::Parser)]
/// clap 은 명령줄 인터페이스를 만드는 편리한 방법을 제공한다.
/// parser 구조체 또는 열거형에 매크로 추가해서 개발자는 선언적 방식으로 프로그램의 명령줄 인수 및 옵션을 지정할 수 있음.
pub(super) struct RunCmd {
    /// Configure node to run as archival node which prevents deletion of old
    /// blocks.  This is a persistent setting; once client is started as
    /// archival node, it cannot be run in non-archival mode.
    #[clap(long)]
    archive: bool,
    /// Set the boot nodes to bootstrap network from.
    #[clap(long)]
    boot_nodes: Option<String>,
    /// Whether to re-establish connections from the ConnectionStore on startup
    #[clap(long)]
    connect_to_reliable_peers_on_startup: Option<bool>,
    /// Minimum number of peers to start syncing/producing blocks
    #[clap(long)]
    min_peers: Option<usize>,
    /// Customize network listening address (useful for running multiple nodes on the same machine).
    #[clap(long)]
    network_addr: Option<SocketAddr>,
    /// Set this to false to only produce blocks when there are txs or receipts (default true).
    #[clap(long)]
    produce_empty_blocks: Option<bool>,
    /// Customize RPC listening address (useful for running multiple nodes on
    /// the same machine).  Ignored if ‘--disable-rpc’ is given.
    #[cfg(feature = "json_rpc")]
    #[clap(long)]
    rpc_addr: Option<String>,
    /// Export prometheus metrics on an additional listening address, which is useful
    /// for having separate access restrictions for the RPC and prometheus endpoints.
    /// Ignored if RPC http server is disabled, see 'rpc_addr'.
    #[cfg(feature = "json_rpc")]
    #[clap(long)]
    rpc_prometheus_addr: Option<String>,
    /// Disable the RPC endpoint.  This is a no-op on builds which don’t support
    /// RPC endpoint.
    #[clap(long)]
    #[allow(dead_code)]
    disable_rpc: bool,
    /// Customize telemetry url.
    #[clap(long)]
    telemetry_url: Option<String>,
    /// Customize max_gas_burnt_view runtime limit.  If not specified, either
    /// value given at ‘init’ (i.e. present in config.json) or one from genesis
    /// configuration will be taken.
    #[clap(long)]
    max_gas_burnt_view: Option<Gas>,
}

impl RunCmd {
    pub(super) fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
        verbose_target: Option<&str>,
        o11y_opts: &near_o11y::Options,
    ) {
        // Load configs from home.
        let mut near_config = nearcore::config::load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        check_release_build(&near_config.client_config.chain_id);

        // Set current version in client config.
        near_config.client_config.version = crate::neard_version();
        // Override some parameters from command line.
        if let Some(produce_empty_blocks) = self.produce_empty_blocks {
            near_config.client_config.produce_empty_blocks = produce_empty_blocks;
        }
        if let Some(connect_to_reliable_peers_on_startup) =
        self.connect_to_reliable_peers_on_startup
        {
            near_config.network_config.connect_to_reliable_peers_on_startup =
                connect_to_reliable_peers_on_startup;
        }
        if let Some(boot_nodes) = self.boot_nodes {
            if !boot_nodes.is_empty() {
                near_config.network_config.peer_store.boot_nodes = boot_nodes
                    .split(',')
                    .map(|chunk| chunk.parse().expect("Failed to parse PeerInfo"))
                    .collect();
            }
        }
        if let Some(min_peers) = self.min_peers {
            near_config.client_config.min_num_peers = min_peers;
        }
        if let Some(network_addr) = self.network_addr {
            near_config.network_config.node_addr =
                Some(near_network::tcp::ListenerAddr::new(network_addr));
        }
        #[cfg(feature = "json_rpc")]
        if self.disable_rpc {
            near_config.rpc_config = None;
        } else {
            if let Some(rpc_addr) = self.rpc_addr {
                near_config.rpc_config.get_or_insert(Default::default()).addr =
                    tcp::ListenerAddr::new(rpc_addr.parse().unwrap());
            }
            if let Some(rpc_prometheus_addr) = self.rpc_prometheus_addr {
                near_config.rpc_config.get_or_insert(Default::default()).prometheus_addr =
                    Some(rpc_prometheus_addr);
            }
        }
        if let Some(telemetry_url) = self.telemetry_url {
            if !telemetry_url.is_empty() {
                near_config.telemetry_config.endpoints.push(telemetry_url);
            }
        }
        if self.archive {
            near_config.client_config.archive = true;
        }
        if self.max_gas_burnt_view.is_some() {
            near_config.client_config.max_gas_burnt_view = self.max_gas_burnt_view;
        }

        #[cfg(feature = "sandbox")]
        {
            if near_config.client_config.chain_id == "mainnet"
                || near_config.client_config.chain_id == "testnet"
                || near_config.client_config.chain_id == "betanet"
            {
                eprintln!(
                    "Sandbox node can only run dedicate localnet, cannot connect to a network"
                );
                std::process::exit(1);
            }
        }

        let (tx_crash, mut rx_crash) = broadcast::channel::<()>(16);
        let (tx_config_update, rx_config_update) =
            broadcast::channel::<Result<UpdateableConfigs, Arc<UpdateableConfigLoaderError>>>(16);
        let sys = actix::System::new();

        sys.block_on(async move {
            // Initialize the subscriber that takes care of both logging and tracing.
            let _subscriber_guard = default_subscriber_with_opentelemetry(
                make_env_filter(verbose_target).unwrap(),
                o11y_opts,
                near_config.client_config.chain_id.clone(),
                near_config.network_config.node_key.public_key().clone(),
                near_config
                    .network_config
                    .validator
                    .as_ref()
                    .map(|validator| validator.account_id()),
            )
                .await
                .global();

            let updateable_configs = nearcore::dyn_config::read_updateable_configs(home_dir)
                .unwrap_or_else(|e| panic!("Error reading dynamic configs: {:#}", e));
            let mut updateable_config_loader =
                UpdateableConfigLoader::new(updateable_configs.clone(), tx_config_update);
            let config_updater = ConfigUpdater::new(rx_config_update);

            let nearcore::NearNode {
                rpc_servers,
                cold_store_loop_handle,
                state_sync_dump_handle,
                ..
            } = nearcore::start_with_config_and_synchronization(
                home_dir,
                near_config,
                Some(tx_crash),
                Some(config_updater),
            )
                .expect("start_with_config");

            let sig = loop {
                let sig = wait_for_interrupt_signal(home_dir, &mut rx_crash).await;
                if sig == "SIGHUP" {
                    let maybe_updateable_configs =
                        nearcore::dyn_config::read_updateable_configs(home_dir);
                    updateable_config_loader.reload(maybe_updateable_configs);
                } else {
                    break sig;
                }
            };
            warn!(target: "neard", "{}, stopping... this may take a few minutes.", sig);
            if let Some(handle) = cold_store_loop_handle {
                handle.stop()
            }
            if let Some(handle) = state_sync_dump_handle {
                handle.stop()
            }
            futures::future::join_all(rpc_servers.iter().map(|(name, server)| async move {
                server.stop(true).await;
                debug!(target: "neard", "{} server stopped", name);
            }))
                .await;
            actix::System::current().stop();
            // Disable the subscriber to properly shutdown the tracer.
            near_o11y::reload(Some("error"), None, Some(near_o11y::OpenTelemetryLevel::OFF))
                .unwrap();
        });
        sys.run().unwrap();
        info!(target: "neard", "Waiting for RocksDB to gracefully shutdown");
        RocksDB::block_until_all_instances_are_dropped();
    }
}

#[cfg(not(unix))]
async fn wait_for_interrupt_signal(_home_dir: &Path, mut _rx_crash: &Receiver<()>) -> &str {
    // TODO(#6372): Support graceful shutdown on windows.
    tokio::signal::ctrl_c().await.unwrap();
    "Ctrl+C"
}

#[cfg(unix)]
async fn wait_for_interrupt_signal(_home_dir: &Path, rx_crash: &mut Receiver<()>) -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sighup = signal(SignalKind::hangup()).unwrap();

    tokio::select! {
         _ = sigint.recv()  => "SIGINT",
         _ = sigterm.recv() => "SIGTERM",
         _ = sighup.recv() => "SIGHUP",
         _ = rx_crash.recv() => "ClientActor died",
    }
}

#[derive(clap::Parser)]
/// clap 라이브러리에서 제공하는 파서 매크로 - 커맨드라인 인터페이스 쉽게 구현할 수 있도록 함.
pub(super) struct LocalnetCmd {
    /// near 노드의 로컬넷 초기화함.

    /// Number of non-validators to initialize the localnet with.
    /// 로컬넷을 초기화하기 위해 non validator 노드의 수를 지정, 기본값 0.
    #[clap(short = 'n', long, alias = "n", default_value = "0")]
    non_validators: NumSeats,
    /// Prefix for the directory name for each node with (e.g. ‘node’ results in
    /// ‘node0’, ‘node1’, ...)
    /// prefix 인자는 로컬넷에서 각 노드 디렉토리의 이름을 지정하는 데 사용됨
    /// prefix 인자에 지정한 문자열과 각 노드의 번호를 조합해 노드 디렉토리 이름을 생성함.
    /// 노드 디렉토리 이름 생성하는 이유?
    /// 로컬넷에서 각 노드를 독립적으로 샐행하기 위해, 각 노드는 자체 디렉토리에서 실행됨. 서로간의 충돌 방지할 수 있음.
    /// 로컬넷에서 여러 노드를 실행할 때 각 노드를 쉽게 구분할 수 있도록 노드 디렉토리 이름을 지정하는 것이 좋음.
    #[clap(long, default_value = "node")]
    prefix: String,
    /// Number of shards to initialize the localnet with.
    #[clap(short = 's', long, default_value = "1")]
    shards: NumShards,
    /// Number of validators to initialize the localnet with.
    #[clap(short = 'v', long, alias = "v", default_value = "4")]
    validators: NumSeats,
    /// Whether to create fixed shards accounts (that are tied to a given
    /// shard).
    #[clap(long)]
    fixed_shards: bool,
    /// Whether to configure nodes as archival.
    #[clap(long)]
    archival_nodes: bool,
    /// Comma separated list of shards to track, the word 'all' to track all shards or the word 'none' to track no shards.
    #[clap(long, default_value = "all")]
    tracked_shards: String,
}

impl LocalnetCmd {
    fn parse_tracked_shards(tracked_shards: &str, num_shards: NumShards) -> Vec<u64> {
        if tracked_shards.to_lowercase() == "all" {
            return (0..num_shards).collect();
        }
        if tracked_shards.to_lowercase() == "none" {
            return vec![];
        }
        tracked_shards
            .split(',')
            .map(|shard_id| shard_id.parse::<u64>().expect("Shard id must be an integer"))
            .collect()
    }

    pub(super) fn run(self, home_dir: &Path) {
        let tracked_shards = Self::parse_tracked_shards(&self.tracked_shards, self.shards);

        nearcore::config::init_testnet_configs(
            home_dir,
            self.shards,
            self.validators,
            self.non_validators,
            &self.prefix,
            true,
            self.archival_nodes,
            self.fixed_shards,
            tracked_shards,
        );
    }
}

#[derive(clap::Args)]
#[clap(arg_required_else_help = true)]
pub(super) struct RecompressStorageSubCommand {
    /// Directory where to save new storage.
    #[clap(long)]
    output_dir: PathBuf,

    /// Keep data in DBCol::PartialChunks column.  Data in that column can be
    /// reconstructed from DBCol::Chunks is not needed by archival nodes.  This is
    /// always true if node is not an archival node.
    #[clap(long)]
    keep_partial_chunks: bool,

    /// Keep data in DBCol::InvalidChunks column.  Data in that column is only used
    /// when receiving chunks and is not needed to serve archival requests.
    /// This is always true if node is not an archival node.
    #[clap(long)]
    keep_invalid_chunks: bool,

    /// Keep data in DBCol::TrieChanges column.  Data in that column is never used
    /// by archival nodes.  This is always true if node is not an archival node.
    #[clap(long)]
    keep_trie_changes: bool,
}

impl RecompressStorageSubCommand {
    pub(super) fn run(self, home_dir: &Path) {
        warn!(target: "neard", "Recompressing storage; note that this operation may take up to a day to finish.");
        let opts = nearcore::RecompressOpts {
            dest_dir: self.output_dir,
            keep_partial_chunks: self.keep_partial_chunks,
            keep_invalid_chunks: self.keep_invalid_chunks,
            keep_trie_changes: self.keep_trie_changes,
        };
        if let Err(err) = nearcore::recompress_storage(home_dir, opts) {
            error!("{}", err);
            std::process::exit(1);
        }
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum VerifyProofError {
    #[error("invalid outcome root proof")]
    InvalidOutcomeRootProof,
    #[error("invalid block hash proof")]
    InvalidBlockHashProof,
}

#[derive(clap::Parser)]
pub struct VerifyProofSubCommand {
    #[clap(long)]
    json_file_path: String,
}

impl VerifyProofSubCommand {
    /// Verifies light client transaction proof (result of the EXPERIMENTAL_light_client_proof RPC call).
    /// Returns the Hash and height of the block that transaction belongs to, and root of the light block merkle tree.
    pub fn run(self) -> ((CryptoHash, u64), CryptoHash) {
        let file = File::open(Path::new(self.json_file_path.as_str()))
            .with_context(|| "Could not open proof file.")
            .unwrap();
        let reader = BufReader::new(file);
        let light_client_rpc_response: Value =
            serde_json::from_reader(reader).with_context(|| "Failed to deserialize JSON.").unwrap();
        Self::verify_json(light_client_rpc_response).unwrap()
    }

    pub fn verify_json(
        light_client_rpc_response: Value,
    ) -> Result<((CryptoHash, u64), CryptoHash), VerifyProofError> {
        let light_client_proof: RpcLightClientExecutionProofResponse =
            serde_json::from_value(light_client_rpc_response["result"].clone()).unwrap();

        println!(
            "Verifying light client proof for txn id: {:?}",
            light_client_proof.outcome_proof.id
        );
        let outcome_hashes = light_client_proof.outcome_proof.to_hashes();
        println!("Hashes of the outcome are: {:?}", outcome_hashes);

        let outcome_hash = CryptoHash::hash_borsh(&outcome_hashes);
        println!("Hash of the outcome is: {:?}", outcome_hash);

        let outcome_shard_root =
            compute_root_from_path(&light_client_proof.outcome_proof.proof, outcome_hash);
        println!("Shard outcome root is: {:?}", outcome_shard_root);
        let block_outcome_root = compute_root_from_path(
            &light_client_proof.outcome_root_proof,
            CryptoHash::hash_borsh(outcome_shard_root),
        );
        println!("Block outcome root is: {:?}", block_outcome_root);

        if light_client_proof.block_header_lite.inner_lite.outcome_root != block_outcome_root {
            println!(
                "{}",
                ansi_term::Colour::Red.bold().paint(format!(
                    "ERROR: computed outcome root: {:?} doesn't match the block one {:?}.",
                    block_outcome_root,
                    light_client_proof.block_header_lite.inner_lite.outcome_root
                ))
            );
            return Err(VerifyProofError::InvalidOutcomeRootProof);
        }
        let block_hash = light_client_proof.outcome_proof.block_hash;

        if light_client_proof.block_header_lite.hash()
            != light_client_proof.outcome_proof.block_hash
        {
            println!("{}",
                     ansi_term::Colour::Red.bold().paint(format!(
                         "ERROR: block hash from header lite {:?} doesn't match the one from outcome proof {:?}",
                         light_client_proof.block_header_lite.hash(),
                         light_client_proof.outcome_proof.block_hash
                     )));
            return Err(VerifyProofError::InvalidBlockHashProof);
        } else {
            println!(
                "{}",
                ansi_term::Colour::Green
                    .bold()
                    .paint(format!("Block hash matches {:?}", block_hash))
            );
        }

        // And now check that block exists in the light client.

        let light_block_merkle_root =
            compute_root_from_path(&light_client_proof.block_proof, block_hash);

        println!(
            "Please verify that your light block has the following block merkle root: {:?}",
            light_block_merkle_root
        );
        println!(
            "OR verify that block with this hash {:?} is in the chain at this heigth {:?}",
            block_hash, light_client_proof.block_header_lite.inner_lite.height
        );
        Ok((
            (block_hash, light_client_proof.block_header_lite.inner_lite.height),
            light_block_merkle_root,
        ))
    }
}

fn make_env_filter(verbose: Option<&str>) -> Result<EnvFilter, BuildEnvFilterError> {
    let env_filter = EnvFilterBuilder::from_env().verbose(verbose).finish()?;
    // Sandbox node can log to sandbox logging target via sandbox_debug_log host function.
    // This is hidden by default so we enable it for sandbox node.
    let env_filter = if cfg!(feature = "sandbox") {
        env_filter.add_directive("sandbox=debug".parse().unwrap())
    } else {
        env_filter
    };
    Ok(env_filter)
}

#[derive(clap::Parser)]
pub(super) struct ValidateConfigCommand {}

impl ValidateConfigCommand {
    pub(super) fn run(&self, home_dir: &Path) -> anyhow::Result<()> {
        nearcore::config::load_config(home_dir, GenesisValidationMode::Full)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CryptoHash, NeardCmd, NeardSubCommand, VerifyProofError, VerifyProofSubCommand};
    use clap::Parser;
    use std::str::FromStr;

    #[test]
    fn optional_values() {
        let cmd = NeardCmd::parse_from(&["test", "init", "--chain-id=testid", "--fast"]);
        if let NeardSubCommand::Init(scmd) = cmd.subcmd {
            assert_eq!(scmd.chain_id, Some("testid".to_string()));
            assert!(scmd.fast);
        } else {
            panic!("incorrect subcommand");
        }
    }

    #[test]
    fn equal_no_value_syntax() {
        assert!(NeardCmd::try_parse_from(&[
            "test",
            "init",
            // * This line currently fails to be parsed (= without a value)
            "--chain-id=",
            "--test-seed=alice.near",
            "--account-id=test.near",
            "--fast"
        ])
            .is_err());
    }

    #[test]
    fn verify_proof_test() {
        assert_eq!(
            VerifyProofSubCommand::verify_json(
                serde_json::from_slice(include_bytes!("../res/proof_example.json")).unwrap()
            )
                .unwrap(),
            (
                (
                    CryptoHash::from_str("HqZHDTHSqH6Az22SZgFUjodGFDtfC2qSt4v9uYFpLuFC").unwrap(),
                    38 as u64
                ),
                CryptoHash::from_str("BWwZdhAhjAgKxZ5ycqn1CvXads5DjPMfj4kRdc1rWit8").unwrap()
            )
        );

        // Proof with a wrong outcome (as user specified wrong shard).
        assert_eq!(
            VerifyProofSubCommand::verify_json(
                serde_json::from_slice(include_bytes!("../res/invalid_proof.json")).unwrap()
            )
                .unwrap_err(),
            VerifyProofError::InvalidOutcomeRootProof
        );
    }
}
