CREATE TABLE IF NOT EXISTS fct_mle_user_login (
    id BIGSERIAL,
    user_id BIGINT,
    login_date DATE,
    PRIMARY KEY (user_id, login_date)
);