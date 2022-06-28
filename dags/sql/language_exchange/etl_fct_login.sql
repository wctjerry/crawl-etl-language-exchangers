-- Step One: Ensure idempotency
DELETE FROM fct_mle_user_login
WHERE login_date = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}';

-- Step Two: Insert new records
INSERT INTO fct_mle_user_login (
    id,
    login_date
)

SELECT u.id,
        l.last_login
FROM staging_my_launguage_exchange l
    JOIN dim_mle_users u ON u.is_current = True
                        AND u.user_id = l.user_id
WHERE last_login = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}'
ON CONFLICT (id, login_date) DO NOTHING;
