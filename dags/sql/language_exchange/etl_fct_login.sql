INSERT INTO fct_mle_user_login (
    user_id,
    login_date
)

SELECT user_id,
        last_login

FROM staging_my_launguage_exchange

WHERE last_login = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}'

ON CONFLICT (user_id, login_date) DO NOTHING;


