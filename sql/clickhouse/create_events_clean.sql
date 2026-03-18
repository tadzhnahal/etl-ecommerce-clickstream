create table if not exists analytics.dm_events_clean (
    event_time DateTime,
    event_type String,
    product_id Int64,
    category_id Nullable(Int64),
    category_code Nullable(String),
    brand Nullable(String),
    price Nullable(Decimal(14, 2)),
    user_id Nullable(Int64),
    user_session Nullable(String),
    event_date Date,
    event_hour Int32,
    day_of_week Int32,
    category_level_1 Nullable(String),
    category_level_2 Nullable(String),
    category_level_3 Nullable(String)
)
engine = MergeTree
ORDER by (event_date, event_time, product_id);