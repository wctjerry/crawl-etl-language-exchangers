INSERT INTO dim_mle_users (
    id,
    name,
    country,
    native_language,
    practicing_language,
    description,
    image_url
)

SELECT user_id,
        name,
        country,
        native_language,
        practicing_language,
        description,
        image_url

FROM staging_my_launguage_exchange

WHERE last_login = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}'

ON CONFLICT (id) DO UPDATE SET
    name = excluded.name,
    country = excluded.country,
    native_language = excluded.native_language,
    practicing_language = excluded.practicing_language,
    description = excluded.description,
    image_url = excluded.image_url;
