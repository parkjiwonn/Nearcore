use crate::metrics;
use chrono::{DateTime, Utc};
use near_primitives::static_clock::StaticClock;
use near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// A wrapper for a config value that can be updated while the node is running.
/// When initializing sub-objects (e.g. `ShardsManager`), please make sure to
/// pass this wrapper instead of passing a value from a single moment in time.
/// See `expected_shutdown` for an example how to use it.
///
/// 노드가 실행되는 동안 업데이트할 수 있는 설정 값의 래퍼입니다.
/// 서브 오브젝트(예: `ShardsManager`)를 초기화할 때는 특정 시점의 값을 전달하는 대신 이 래퍼를 전달해야 합니다.
///사용 예는 `expected_shutdown`을 참조하세요.
///
/// 정리 : Mutable 변할 수 있는 ConfigValue 설정값.
/// 노드가 실행중일때 수정되는 설정 값을 싼다
/// 초기화할 때 특정 시점의 값 전달해야 할 때 wrapper을 전달하는 것.

#[derive(Clone)]
pub struct MutableConfigValue<T> {
    value: Arc<Mutex<T>>,
    // For metrics.
    // Mutable config values are exported to prometheus with labels [field_name][last_update][value].
    field_name: String,
    // For metrics.
    // Mutable config values are exported to prometheus with labels [field_name][last_update][value].
    last_update: DateTime<Utc>,
}
///
impl<T: Serialize> Serialize for MutableConfigValue<T> {
    /// Only include the value field of MutableConfigValue in serialized result
    /// since field_name and last_update are only relevant for internal monitoring
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let to_string_result = serde_json::to_string(&self.value);
        let value_str = to_string_result.unwrap_or("unable to serialize the value".to_string());
        serializer.serialize_str(&value_str)
    }
}

impl<T: Copy + PartialEq + Debug> MutableConfigValue<T> {
    /// Initializes a value.
    /// 값을 초기화한다.
    /// `field_name` is needed to export the config value as a prometheus metric.
    /// file_nema은 프로메테우스 메트릭으로써 설정 값을 수출하기 위해 필요하다.
    pub fn new(val: T, field_name: &str) -> Self {
        let res = Self {
            value: Arc::new(Mutex::new(val)),
            field_name: field_name.to_string(),
            last_update: StaticClock::utc(),
        };
        res.set_metric_value(val, 1);
        res
    }

    pub fn get(&self) -> T {
        /// 현재 설정값 가져오기.
        *self.value.lock().unwrap()
    }

    pub fn update(&self, val: T) {
        /// 설정값 업데이트
        let mut lock = self.value.lock().unwrap();
        if *lock != val {
            tracing::info!(target: "config", "Updated config field '{}' from {:?} to {:?}", self.field_name, *lock, val);
            self.set_metric_value(*lock, 0);
            *lock = val;
            self.set_metric_value(val, 1);
        } else {
            /// INFO 찍힌 것는 [ Mutable config field 'expected_shutdown' remains the same: None ] 로 찍힘.
            tracing::info!(target: "config", "Mutable config field '{}' remains the same: {:?}", self.field_name, val);
        }
    }

    fn set_metric_value(&self, value: T, metric_value: i64) {
        // Use field_name as a label to tell different mutable config values apart.
        /// 필드 이름을 레이블로 사용해 서로 다른 변경 가능한 설정 값을 구분한다.
        // Use timestamp as a label to give some idea to the node operator (or
        // people helping them debug their node) when exactly and what values
        // exactly were part of the config.
        /// 타임 스탬프를 레이블로 사용해 노드 운영자에게 정확히 언제 어떤 값이 구성에 포함되었는지 알 수 있다.
        // Use the config value as a label to make this work with config values
        // of any type: int, float, string or even a composite object.
        /// 설정값을 레이블로 사용하면 정수, 부동 소수점, 문자열 또는 복합 객체 등 모든 유형의 설정 값에서 작동한다.
        metrics::CONFIG_MUTABLE_FIELD
            .with_label_values(&[
                &self.field_name,
                &self.last_update.timestamp().to_string(),
                &format!("{:?}", value),
            ])
            .set(metric_value);
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
/// A subset of Config that can be updated white the node is running.
///
pub struct UpdateableClientConfig {
    /// Graceful shutdown at expected block height.
    pub expected_shutdown: Option<BlockHeight>,
}
