DROP TABLE IF EXISTS {{ params.tb_name }};

CREATE TABLE IF NOT EXISTS {{ params.tb_name }} (
    city VARCHAR,
    country VARCHAR,
    description VARCHAR,
    image_url VARCHAR,
    last_login DATE,
    name VARCHAR,
    native_language VARCHAR,
    practicing_language VARCHAR,
    user_id BIGINT
);

SELECT aws_s3.table_import_from_s3(
    '{{ params.tb_name }}',
    '',
    '(format csv, HEADER true)',
    '{{ ti.xcom_pull(
        task_ids="scrape_operator_language_exchange_task",
        key="bucket",
    ) }}',
    '{{ ti.xcom_pull(
        task_ids="scrape_operator_language_exchange_task",
        key="object_name",
    ) }}',
    '{{ params.bucket_region }}'
);
