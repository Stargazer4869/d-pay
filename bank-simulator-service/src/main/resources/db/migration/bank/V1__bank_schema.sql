create schema if not exists bank;

create table if not exists bank.transactions (
    operation_key varchar(128) primary key,
    operation_type varchar(32) not null,
    account_ref varchar(255) not null,
    amount_minor bigint not null,
    currency varchar(3) not null,
    reference varchar(255) not null,
    status varchar(64) not null,
    attempt_count integer not null,
    failure_budget integer not null,
    terminal boolean not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);
