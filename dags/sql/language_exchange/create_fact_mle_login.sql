CREATE TABLE IF NOT EXISTS fct_mle_user_login (
    id BIGINT,
    login_date DATE,
    PRIMARY KEY (id, login_date)
);