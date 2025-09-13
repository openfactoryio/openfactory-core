-- ---------------------------------------------------------------------
-- Connector configuration for devices

-- Base definition (backed by Kafka topic, not directly queryable)
CREATE TABLE DEVICE_CONNECTOR_SOURCE (
    ASST_UUID STRING PRIMARY KEY,
    CONNECTOR_CONFIG STRING
) WITH (
    KAFKA_TOPIC='device_connector_topic',
    VALUE_FORMAT='JSON',
    PARTITIONS=1
);

-- Materialized version (queryable)
CREATE TABLE DEVICE_CONNECTOR AS
    SELECT ASST_UUID, CONNECTOR_CONFIG
    FROM DEVICE_CONNECTOR_SOURCE
    EMIT CHANGES;
