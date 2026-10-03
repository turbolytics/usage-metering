-- The finest grain: one row per minute, customer, meter and Kafka partition.
-- The count workers upsert on this key, so a republished minute replaces its
-- row. Readers sum over kafka_partition. The rollup tables usage_by_customer_1h
-- and usage_by_customer_1d are created by `sqlflow rollup install`, not here.
CREATE TABLE IF NOT EXISTS usage_per_minute (
  minute          TIMESTAMPTZ NOT NULL,
  customer        TEXT        NOT NULL,
  meter           TEXT        NOT NULL,
  kafka_partition INTEGER     NOT NULL,
  quantity        BIGINT      NOT NULL,
  PRIMARY KEY (minute, customer, meter, kafka_partition)
);
