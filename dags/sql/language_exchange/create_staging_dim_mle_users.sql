DROP TABLE IF EXISTS staging_changed_mle_users;

CREATE TABLE staging_changed_mle_users AS
SELECT user_id,
        COALESCE(name, '') AS name,
        COALESCE(country, '') AS country,
        COALESCE(native_language, '') AS native_language,
        COALESCE(practicing_language, '') AS practicing_language,
        description,
        image_url,
        True AS is_changed, -- Newly add or updated
        '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}' AS changed_date
FROM staging_my_launguage_exchange s
WHERE s.last_login = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}'
    AND NOT EXISTS (
        SELECT 1
        FROM dim_mle_users u
        WHERE u.user_id = s.user_id
            AND u.name = s.name
            AND u.country = s.country
            AND u.native_language = s.native_language
            AND u.practicing_language = s.practicing_language
            AND '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -2) }}'::DATE -- For idempotency
                BETWEEN u.effective_date AND u.expiration_date
);
