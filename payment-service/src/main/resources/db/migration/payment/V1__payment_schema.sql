create schema if not exists payment;

create table if not exists payment.payments (
    id uuid primary key,
    merchant_id varchar(128) not null,
    merchant_reference varchar(255) not null,
    amount_minor bigint not null,
    currency varchar(3) not null,
    payer_ref varchar(255) not null,
    payee_ref varchar(255) not null,
    status varchar(64) not null,
    processing_attempts integer not null,
    last_error text,
    expires_at timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists payment.refunds (
    id uuid primary key,
    payment_id uuid not null references payment.payments (id),
    merchant_id varchar(128) not null,
    amount_minor bigint not null,
    currency varchar(3) not null,
    status varchar(64) not null,
    processing_attempts integer not null,
    last_error text,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists payment.payment_events (
    id bigserial primary key,
    payment_id uuid not null references payment.payments (id),
    merchant_id varchar(128) not null,
    event_type varchar(64) not null,
    status varchar(64) not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null
);

create table if not exists payment.refund_events (
    id bigserial primary key,
    refund_id uuid not null references payment.refunds (id),
    payment_id uuid not null references payment.payments (id),
    merchant_id varchar(128) not null,
    event_type varchar(64) not null,
    status varchar(64) not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null
);

create table if not exists payment.idempotency_records (
    id bigserial primary key,
    merchant_id varchar(128) not null,
    operation varchar(64) not null,
    idempotency_key varchar(128) not null,
    response_body jsonb not null,
    created_at timestamptz not null,
    unique (merchant_id, operation, idempotency_key)
);

create table if not exists payment.outbox_events (
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

create index if not exists payment_payments_merchant_idx
    on payment.payments (merchant_id, created_at desc);

create index if not exists payment_refunds_payment_idx
    on payment.refunds (payment_id, created_at desc);
