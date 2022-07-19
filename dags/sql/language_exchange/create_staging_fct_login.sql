DROP TABLE IF EXISTS staging_changed_mle_login;

CREATE TABLE staging_changed_mle_login AS
SELECT user_id,
        last_login
FROM staging_my_launguage_exchange
WHERE last_login = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}';
