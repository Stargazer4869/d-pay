create schema if not exists ledger;

create table if not exists ledger.accounts (
    merchant_id varchar(128) not null,
    currency varchar(3) not null,
    available_balance bigint not null,
    reserved_outgoing bigint not null,
    reserved_refund bigint not null,
    updated_at timestamptz not null,
    primary key (merchant_id, currency)
);

create table if not exists ledger.operation_log (
    merchant_id varchar(128) not null,
    currency varchar(3) not null,
    operation_type varchar(64) not null,
    operation_id varchar(128) not null,
    created_at timestamptz not null,
    primary key (merchant_id, currency, operation_type, operation_id)
);

create table if not exists ledger.journal_entries (
    id uuid primary key,
    merchant_id varchar(128) not null,
    currency varchar(3) not null,
    operation_type varchar(64) not null,
    operation_id varchar(160) not null,
    balance_bucket varchar(64) not null,
    amount_delta bigint not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null
);

create index if not exists ledger_journal_entries_merchant_idx
    on ledger.journal_entries (merchant_id, currency, created_at desc);
