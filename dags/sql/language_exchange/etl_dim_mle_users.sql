-- https://www.reddit.com/r/PostgreSQL/comments/q4ya4w/cte_execution_order_delete_after_inserting/
-- Step One: Ensure idempotency. Delete newly inserted and rollback updated rows
WITH updated_in_this_interval AS (
    SELECT user_id
    FROM dim_mle_users u
    WHERE u.effective_date = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}'::DATE
),

delete_newly_inserted AS (
    DELETE FROM dim_mle_users u
    WHERE u.effective_date = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -1) }}'::DATE
        AND u.is_current = True
)

UPDATE dim_mle_users u
SET is_current = True,
    expiration_date = '9999-12-31'::DATE
FROM updated_in_this_interval up
WHERE u.user_id = up.user_id
    AND u.is_current = False;

-- Step Two: update changes
WITH update_old AS (
    UPDATE dim_mle_users u
    SET is_current = False,
        expiration_date = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -2) }}'::DATE -- Previous day
    FROM staging_changed_mle_users c
    WHERE u.user_id = c.user_id
        AND u.is_current = True
)
INSERT INTO dim_mle_users (
    user_id,
    name,
    country,
    native_language,
    practicing_language,
    description,
    image_url,
    is_current,
    effective_date,
    expiration_date
)
SELECT user_id,
        name,
        country,
        native_language,
        practicing_language,
        description,
        image_url,
        True AS is_current,
        changed_date::DATE AS effective_date,
        '9999-12-31'::DATE AS expiration_date
FROM staging_changed_mle_users;
