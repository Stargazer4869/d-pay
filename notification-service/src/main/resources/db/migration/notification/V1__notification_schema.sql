create schema if not exists platform;
create schema if not exists notification;

create table if not exists platform.webhook_subscriptions (
    merchant_id varchar(128) primary key,
    target_url text not null,
    secret varchar(255) not null,
    active boolean not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists notification.inbox_events (
    event_id uuid primary key,
    processed_at timestamptz not null
);

create table if not exists notification.webhook_deliveries (
    event_id uuid not null,
    merchant_id varchar(128) not null,
    target_url text not null,
    secret varchar(255) not null,
    payload text not null,
    status varchar(32) not null,
    attempt_count integer not null,
    last_response_code integer,
    last_error text,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    next_attempt_at timestamptz not null,
    primary key (event_id, merchant_id, target_url)
);
