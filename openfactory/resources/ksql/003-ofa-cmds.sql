-- OpenFactory cmds stream
CREATE STREAM cmds_stream (
        device_uuid VARCHAR KEY,
        cmd VARCHAR,
        args VARCHAR
    ) WITH (
        KAFKA_TOPIC = 'ofa_cmds',
        VALUE_FORMAT = 'JSON'
    );
