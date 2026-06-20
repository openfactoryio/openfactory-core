-- ---------------------------------------------------------------------
-- OpenFactory Metrics Registry

-- Source table
CREATE TABLE IF NOT EXISTS METRICS_TARGETS_SOURCE (
    APPLICATION_UUID STRING PRIMARY KEY,
    HOST STRING,
    PORT INTEGER,
    PATH STRING
) WITH (
    KAFKA_TOPIC='metrics_targets_topic',
    VALUE_FORMAT='JSON',
    PARTITIONS=1
);

-- Materialized table
CREATE TABLE IF NOT EXISTS METRICS_TARGETS AS
    SELECT
        APPLICATION_UUID,
        HOST,
        PORT,
        PATH
    FROM METRICS_TARGETS_SOURCE
    EMIT CHANGES;
