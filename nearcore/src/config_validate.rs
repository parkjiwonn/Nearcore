use near_config_utils::{ValidationError, ValidationErrors};

use crate::config::Config;


/// Validate Config extracted from config.json.
/// config.json에서 추출한 Config를 검증한다.
/// This function does not panic. It returns the error if any validation fails.
/// 이 함수는 panic을 일으키지 않아. 유효성 검사에 실패하면 오류를 반환해
pub fn validate_config(config: &Config) -> Result<(), ValidationError> {
    let mut validation_errors = ValidationErrors::new();
    let mut config_validator = ConfigValidator::new(config, &mut validation_errors);
    tracing::info!(target: "config", "Validating Config, extracted from config.json...");
    /// config가 config.json에서 추출되어 유효성 검사가 수행중임을 나타냄.
    config_validator.validate()
    /// validate 메서드 호출해서 구조체의 유효성 검사하고 Result<(), ValidationError> 반환한다.
    /// 만약 유효하지 않은 구조체가 있으면 ValidationError는 해당 오류 수집하고 ValidationError 결과가 반환됨.
    /// 유효한 구조체라면 단순히 Ok(()) 반환함.
}

/// 구조체 유효성을 검사하는 역할
struct ConfigValidator<'a> {
    config: &'a Config,
    validation_errors: &'a mut ValidationErrors,
}

impl<'a> ConfigValidator<'a> {
    fn new(config: &'a Config, validation_errors: &'a mut ValidationErrors) -> Self {
        Self { config, validation_errors }
    } /// ConfigValidator 구조체의 인스턴스 생성할 때 사용. -> Config, ValidationErrors 인자로 바아서 ConfigValidator 구조체 내의 필드로 저장한다.

    fn validate(&mut self) -> Result<(), ValidationError> {
        self.validate_all_conditions();
        self.result_with_full_error()
    }

    /// this function would check all conditions, and add all error messages to ConfigValidator.errors
    /// 해당 함수는 모든 조건을 검사한다. 그리고 모든 에러 메시지를 ConfigValidator.errors에 더한다.
    /// 조건 검사하면서 오류 발생시 push_config_semantics_error 호출해 해당 오류 수집함.
    fn validate_all_conditions(&mut self) {
        if !self.config.archive && self.config.save_trie_changes == Some(false) {
            let error_message = "Configuration with archive = false and save_trie_changes = false is not supported because non-archival nodes must save trie changes in order to do do garbage collection.".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }

        // Checking that if cold storage is configured, trie changes are definitely saved.
        // Unlike in the previous case, None is not a valid option here.
        if self.config.cold_store.is_some() && self.config.save_trie_changes != Some(true) {
            let error_message = format!("cold_store is configured, but save_trie_changes is {:?}. Trie changes should be saved to support cold storage.", self.config.save_trie_changes);
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.consensus.min_block_production_delay
            > self.config.consensus.max_block_production_delay
        {
            let error_message = format!(
                "min_block_production_delay: {:?} is greater than max_block_production_delay: {:?}",
                self.config.consensus.min_block_production_delay,
                self.config.consensus.max_block_production_delay
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.consensus.min_block_production_delay
            > self.config.consensus.max_block_wait_delay
        {
            let error_message = format!(
                "min_block_production_delay: {:?} is greater than max_block_wait_delay: {:?}",
                self.config.consensus.min_block_production_delay,
                self.config.consensus.max_block_wait_delay
            );
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.consensus.header_sync_expected_height_per_second == 0 {
            let error_message =
                "consensus.header_sync_expected_height_per_second should not be 0".to_string();
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if self.config.gc.gc_blocks_limit == 0
            || self.config.gc.gc_fork_clean_step == 0
            || self.config.gc.gc_num_epochs_to_keep == 0
        {
            let error_message = format!("gc config values should all be greater than 0, but gc_blocks_limit is {:?}, gc_fork_clean_step is {}, gc_num_epochs_to_keep is {}.", self.config.gc.gc_blocks_limit, self.config.gc.gc_fork_clean_step, self.config.gc.gc_num_epochs_to_keep);
            self.validation_errors.push_config_semantics_error(error_message);
        }

        if let Some(state_sync) = &self.config.state_sync {
            if state_sync.dump_enabled.unwrap_or(false) {
                if state_sync.s3_bucket.is_empty() || state_sync.s3_region.is_empty() {
                    let error_message = format!("'config.state_sync.s3_bucket' and 'config.state_sync.s3_region' need to be specified when 'config.state_sync.dump_enabled' is enabled.");
                    self.validation_errors.push_config_semantics_error(error_message);
                }
            }
            if state_sync.sync_from_s3_enabled.unwrap_or(false) {
                if state_sync.s3_bucket.is_empty() || state_sync.s3_region.is_empty() {
                    let error_message = format!("'config.state_sync.s3_bucket' and 'config.state_sync.s3_region' need to be specified when 'config.state_sync.sync_from_s3_enabled' is enabled.");
                    self.validation_errors.push_config_semantics_error(error_message);
                }
            }
        }
    }

    fn result_with_full_error(&self) -> Result<(), ValidationError> {
        if self.validation_errors.is_empty() {
            /// 검증 오류가 없다면 Ok(()) 반환함.
            Ok(())
        } else {
            /// 검증 오류가 있다면 오류 메세지 생성
            let full_err_msg = self.validation_errors.generate_error_message_per_type().unwrap();
            Err(ValidationError::ConfigSemanticsError { error_message: full_err_msg })
            ///ConfigSemanticsError 타입으로 ValidationError 반환함.
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[should_panic(expected = "gc config values should all be greater than 0")]
    fn test_gc_config_value_nonzero() {
        let mut config = Config::default();
        config.gc.gc_blocks_limit = 0;
        // set tracked_shards to be non-empty
        config.tracked_shards.push(20);
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Configuration with archive = false and save_trie_changes = false is not supported"
    )]
    fn test_archive_false_save_trie_changes_false() {
        let mut config = Config::default();
        config.archive = false;
        config.save_trie_changes = Some(false);
        // set tracked_shards to be non-empty
        config.tracked_shards.push(20);
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: Configuration with archive = false and save_trie_changes = false is not supported because non-archival nodes must save trie changes in order to do do garbage collection.\\nconfig.json semantic issue: gc config values should all be greater than 0"
    )]
    fn test_multiple_config_validation_errors() {
        let mut config = Config::default();
        config.archive = false;
        config.save_trie_changes = Some(false);
        config.gc.gc_blocks_limit = 0;
        // set tracked_shards to be non-empty
        config.tracked_shards.push(20);
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: cold_store is configured, but save_trie_changes is None. Trie changes should be saved to support cold storage."
    )]
    fn test_cold_store_without_save_trie_changes() {
        let mut config = Config::default();
        config.cold_store = Some(config.store.clone());
        validate_config(&config).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "\\nconfig.json semantic issue: cold_store is configured, but save_trie_changes is Some(false). Trie changes should be saved to support cold storage."
    )]
    fn test_cold_store_with_save_trie_changes_false() {
        let mut config = Config::default();
        config.cold_store = Some(config.store.clone());
        config.save_trie_changes = Some(false);
        validate_config(&config).unwrap();
    }
}
