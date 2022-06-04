CREATE TABLE IF NOT EXISTS dim_mle_users (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    country VARCHAR,
    native_language VARCHAR,
    practicing_language VARCHAR,
    description VARCHAR,
    image_url VARCHAR
);