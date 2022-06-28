CREATE TABLE IF NOT EXISTS dim_mle_users (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    name VARCHAR,
    country VARCHAR,
    native_language VARCHAR,
    practicing_language VARCHAR,
    description VARCHAR,
    image_url VARCHAR,
    is_current BOOLEAN,
    effective_date DATE,
    expiration_date DATE
);
