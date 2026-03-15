create table if not exists raw.events (
	event_time timestamp,
	event_type text,
	product_id bigint,
	category_id bigint,
	category_code text,
	brand text,
	price numeric(14, 2),
	user_id bigint,
	user_session text );
	
