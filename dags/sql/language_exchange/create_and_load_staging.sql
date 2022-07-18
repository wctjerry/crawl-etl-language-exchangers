DROP TABLE IF EXISTS {{ params.tb_name }};

CREATE TABLE IF NOT EXISTS {{ params.tb_name }} (
    city VARCHAR,
    country VARCHAR,
    description VARCHAR,
    image_url VARCHAR,
    last_login DATE,
    name VARCHAR,
    native_language VARCHAR,
    practicing_language VARCHAR,
    user_id BIGINT
);

COPY staging_my_launguage_exchange FROM PROGRAM 'gzip -dc {{ params.source_path }}' DELIMITER ',' CSV HEADER;