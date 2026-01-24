-- OpenFactory cmds stream
CREATE STREAM cmds_stream (
        device_uuid VARCHAR KEY,
        cmd VARCHAR,
        args VARCHAR
    ) WITH (
        KAFKA_TOPIC = 'ofa_cmds'
        REPLICAS = 1,
        VALUE_FORMAT = 'JSON'
    );
