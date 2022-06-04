INSERT INTO fct_mle_user_login (
    user_id,
    login_date
)

SELECT user_id,
        last_login

FROM staging_my_launguage_exchange

WHERE last_login = '{{ ds }}'

ON CONFLICT (user_id, login_date) DO NOTHING;


