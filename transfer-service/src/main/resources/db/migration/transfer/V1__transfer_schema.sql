create schema if not exists platform;
create schema if not exists transfer;

create table if not exists platform.merchant_registry (
    merchant_id varchar(128) primary key,
    shard_id varchar(64) not null,
    created_at timestamptz not null default now()
);

create table if not exists transfer.transfers (
    id uuid primary key,
    source_merchant_id varchar(128) not null,
    destination_merchant_id varchar(128) not null,
    merchant_reference varchar(255) not null,
    amount_minor bigint not null,
    currency varchar(3) not null,
    status varchar(64) not null,
    source_shard_id varchar(64) not null,
    destination_shard_id varchar(64) not null,
    attempt_count integer not null,
    next_retry_at timestamptz not null,
    last_error text,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists transfer.transfer_events (
    id bigserial primary key,
    transfer_id uuid not null references transfer.transfers (id),
    event_type varchar(64) not null,
    status varchar(64) not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null
);

create table if not exists transfer.idempotency_records (
    id bigserial primary key,
    source_merchant_id varchar(128) not null,
    operation varchar(64) not null,
    idempotency_key varchar(128) not null,
    response_body jsonb not null,
    created_at timestamptz not null,
    unique (source_merchant_id, operation, idempotency_key)
);

create table if not exists transfer.outbox_events (
    id uuid primary key,
    aggregate_type varchar(64) not null,
    aggregate_id varchar(128) not null,
    merchant_id varchar(128) not null,
    event_type varchar(64) not null,
    payload jsonb not null,
    published boolean not null default false,
    created_at timestamptz not null,
    published_at timestamptz
);

create index if not exists transfer_transfers_source_idx
    on transfer.transfers (source_merchant_id, created_at desc);

create index if not exists transfer_transfers_destination_idx
    on transfer.transfers (destination_merchant_id, created_at desc);
