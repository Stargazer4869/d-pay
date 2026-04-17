# Payment Gateway Workflow Diagrams

These diagrams describe the current implementation in this repository, not just the target architecture.

Redis is present in the `e2e-tests` environment, but it is not on the active runtime request path today.

## Persistence Ownership

| Component | Persistent dependency | Data owned in the current implementation |
| --- | --- | --- |
| `payment-service` | PostgreSQL shard 1 and shard 2 | `payment.payments`, `payment.refunds`, `payment.payment_events`, `payment.refund_events`, `payment.idempotency_records`, `payment.outbox_events` |
| `transfer-service` | PostgreSQL control | `platform.merchant_registry`, `transfer.transfers`, `transfer.transfer_events`, `transfer.idempotency_records`, `transfer.outbox_events` |
| `ledger-service` | PostgreSQL shard 1 and shard 2 | `ledger.accounts`, `ledger.operation_log`, `ledger.journal_entries` |
| `notification-service` | PostgreSQL control | `platform.webhook_subscriptions`, `notification.inbox_events`, `notification.webhook_deliveries` |
| `bank-simulator-service` | PostgreSQL control | `bank.transactions` |
| `shard-routing` | PostgreSQL control plus shard databases | looks up `platform.merchant_registry` and routes payment and ledger operations to the correct shard |
| `Kafka` | broker only | delivery backbone for outbox-published domain events |

## 1. Runtime Component Map

```mermaid
flowchart LR
  Caller["Merchant / internal caller"]

  subgraph Runtime["Runtime services and libraries"]
    Payment["payment-service"]
    Transfer["transfer-service"]
    Ledger["ledger-service"]
    Notify["notification-service"]
    Bank["bank-simulator-service"]
    Router["shard-routing library"]
    Kafka["Kafka topic: gateway-events"]
  end

  subgraph Storage["Persistence dependencies"]
    ControlDb["PostgreSQL control<br/>schemas: platform, transfer, notification, bank"]
    Shard1["PostgreSQL shard 1<br/>schemas: payment, ledger"]
    Shard2["PostgreSQL shard 2<br/>schemas: payment, ledger"]
  end

  Caller --> Payment
  Caller --> Transfer
  Caller --> Notify

  Payment --> Router
  Ledger --> Router
  Router --> ControlDb
  Router --> Shard1
  Router --> Shard2

  Payment --> Ledger
  Payment --> Bank
  Transfer --> Ledger

  Payment --> Kafka
  Transfer --> Kafka
  Kafka --> Payment
  Kafka --> Notify

  Transfer --> ControlDb
  Notify --> ControlDb
  Bank --> ControlDb
```

## 2. Payment Intent Workflow

```mermaid
sequenceDiagram
  autonumber
  actor Client as "Merchant / caller"
  participant Payment as "payment-service"
  participant Router as "shard-routing"
  participant Control as "PostgreSQL control (platform.merchant_registry)"
  participant PayDb as "PostgreSQL shard N (payment.*)"
  participant Kafka as "Kafka (gateway-events)"
  participant Processor as "payment-service async processor"
  participant Bank as "bank-simulator-service"
  participant Ledger as "ledger-service"
  participant LedgerDb as "PostgreSQL shard N (ledger.*)"

  Client->>Payment: POST /payments
  Payment->>Router: locate shard for merchantId
  Router->>Control: SELECT shard_id
  Control-->>Router: shard N
  Payment->>PayDb: insert payment PENDING<br/>insert payment_events<br/>insert outbox<br/>insert idempotency record
  PayDb-->>Payment: canonical response
  Payment-->>Client: payment PENDING

  Client->>Payment: POST /payments/{id}/confirm
  Payment->>PayDb: lock payment<br/>move to PROCESSING<br/>append event and outbox
  Payment-->>Client: payment PROCESSING

  Note over Payment,Kafka: scheduled outbox publisher emits PAYMENT_CONFIRM_REQUESTED
  Payment->>Kafka: publish event
  Kafka-->>Processor: PAYMENT_CONFIRM_REQUESTED
  Processor->>Bank: withdraw funds from payer

  alt Bank withdraw succeeded
    Processor->>Ledger: credit payment to merchant
    Ledger->>Router: locate shard for merchantId
    Router->>Control: SELECT shard_id
    Control-->>Router: shard N
    Ledger->>LedgerDb: apply PAYMENT_CREDIT<br/>update balances<br/>insert journal<br/>mark operation applied
    Processor->>PayDb: mark payment SUCCEEDED<br/>append payment event and outbox
    Processor->>Kafka: publish PAYMENT_SUCCEEDED
  else Retryable bank failure
    Processor->>PayDb: increment processing attempt<br/>keep PROCESSING or mark FAILED at max attempts
  else Permanent bank failure
    Processor->>PayDb: mark payment FAILED<br/>append payment event and outbox
    Processor->>Kafka: publish PAYMENT_FAILED
  end

  opt Cancel before confirmation completes
    Client->>Payment: POST /payments/{id}/cancel
    Payment->>PayDb: validate PENDING<br/>move to CANCELED<br/>append event<br/>store idempotent response
    Payment-->>Client: payment CANCELED
  end
```

## 3. Refund Workflow

```mermaid
sequenceDiagram
  autonumber
  actor Client as "Merchant / caller"
  participant Payment as "payment-service"
  participant Router as "shard-routing"
  participant Control as "PostgreSQL control (platform.merchant_registry)"
  participant PayDb as "PostgreSQL shard N (payment.*)"
  participant Kafka as "Kafka (gateway-events)"
  participant Processor as "payment-service async processor"
  participant Ledger as "ledger-service"
  participant LedgerDb as "PostgreSQL shard N (ledger.*)"
  participant Bank as "bank-simulator-service"

  Client->>Payment: POST /payments/{id}/refunds
  Payment->>Router: locate shard for merchantId
  Router->>Control: SELECT shard_id
  Control-->>Router: shard N
  Payment->>PayDb: validate refundable payment<br/>check blocked refund total<br/>insert refund PROCESSING<br/>insert refund_events<br/>insert outbox<br/>insert idempotency record
  Payment-->>Client: refund PROCESSING

  Note over Payment,Kafka: scheduled outbox publisher emits REFUND_REQUESTED
  Payment->>Kafka: publish event
  Kafka-->>Processor: REFUND_REQUESTED

  Processor->>Ledger: reserve refund amount on merchant ledger
  Ledger->>Router: locate shard for merchantId
  Router->>Control: SELECT shard_id
  Control-->>Router: shard N
  Ledger->>LedgerDb: move AVAILABLE to RESERVED_REFUND<br/>insert journal<br/>mark REFUND_RESERVE applied

  Processor->>Bank: deposit funds back to payer

  alt Bank deposit succeeded
    Processor->>Ledger: finalize refund reservation
    Ledger->>LedgerDb: consume RESERVED_REFUND<br/>insert journal<br/>mark REFUND_FINALIZE applied
    Processor->>PayDb: mark refund SUCCEEDED<br/>update payment to PARTIALLY_REFUNDED or REFUNDED<br/>append events and outbox
    Processor->>Kafka: publish REFUND_SUCCEEDED
  else Retryable bank failure
    Processor->>PayDb: increment processing attempt<br/>keep PROCESSING or mark FAILED at max attempts
    opt Max attempts reached
      Processor->>Ledger: release reserved refund amount
      Ledger->>LedgerDb: move RESERVED_REFUND back to AVAILABLE
    end
  else Permanent bank failure
    Processor->>Ledger: release reserved refund amount
    Ledger->>LedgerDb: move RESERVED_REFUND back to AVAILABLE
    Processor->>PayDb: mark refund FAILED<br/>append refund event and outbox
    Processor->>Kafka: publish REFUND_FAILED
  end
```

## 4. Cross-Shard Transfer Saga

```mermaid
sequenceDiagram
  autonumber
  actor Client as "Merchant / caller"
  participant Transfer as "transfer-service"
  participant Control as "PostgreSQL control (platform.*, transfer.*)"
  participant Ledger as "ledger-service"
  participant Router as "shard-routing"
  participant SrcDb as "PostgreSQL source shard (ledger.*)"
  participant DstDb as "PostgreSQL destination shard (ledger.*)"
  participant Kafka as "Kafka (gateway-events)"

  Client->>Transfer: POST /transfers
  Transfer->>Control: lookup source and destination shard ids
  Transfer->>Control: insert transfer REQUESTED<br/>insert transfer_events<br/>insert outbox<br/>insert idempotency record
  Transfer-->>Client: transfer REQUESTED

  Note over Transfer,Control: scheduler advances and reconciles sagas from control DB

  Transfer->>Ledger: reserveTransfer(sourceMerchantId, transferId, amount)
  Ledger->>Router: locate source merchant shard
  Router->>Control: SELECT shard_id
  Control-->>Router: source shard id
  Ledger->>SrcDb: decrease AVAILABLE<br/>increase RESERVED_OUTGOING<br/>insert journal<br/>mark TRANSFER_RESERVE applied
  Ledger-->>Transfer: success
  Transfer->>Control: move to FUNDS_RESERVED<br/>append event

  Transfer->>Control: move to CREDITING<br/>append event
  Transfer->>Ledger: creditTransfer(destinationMerchantId, transferId, amount)
  Ledger->>Router: locate destination merchant shard
  Router->>Control: SELECT shard_id
  Control-->>Router: destination shard id

  alt Destination credit succeeded
    Ledger->>DstDb: increase AVAILABLE<br/>insert journal<br/>mark TRANSFER_CREDIT applied
    Ledger-->>Transfer: success
    Transfer->>Ledger: finalizeTransfer(sourceMerchantId, transferId, amount)
    Ledger->>SrcDb: decrease RESERVED_OUTGOING<br/>insert journal<br/>mark TRANSFER_FINALIZE applied
    Transfer->>Control: move to COMPLETED<br/>append event and outbox
    Transfer->>Kafka: publish TRANSFER_COMPLETED
  else Retryable destination credit failure
    Ledger-->>Transfer: retryable failure
    Transfer->>Control: increment attempt_count<br/>set next_retry_at
    Note over Transfer,Control: reconciliation job retries CREDITING later
  else Permanent failure or retry budget exhausted
    Ledger-->>Transfer: failure
    Transfer->>Control: move to COMPENSATING<br/>append event
    Transfer->>Ledger: releaseTransfer(sourceMerchantId, transferId, amount)
    Ledger->>SrcDb: increase AVAILABLE<br/>decrease RESERVED_OUTGOING<br/>insert journal<br/>mark TRANSFER_RELEASE applied
    Transfer->>Control: move to COMPENSATED<br/>append event and outbox
    Transfer->>Kafka: publish TRANSFER_COMPENSATED
  end
```

## 5. Notification And Webhook Delivery

```mermaid
sequenceDiagram
  autonumber
  participant Source as "payment-service or transfer-service"
  participant Kafka as "Kafka (gateway-events)"
  participant Notify as "notification-service"
  participant Control as "PostgreSQL control (platform.*, notification.*)"
  actor Endpoint as "Merchant webhook endpoint"

  Source->>Kafka: publish domain event from outbox
  Kafka-->>Notify: consume event
  Notify->>Control: check notification.inbox_events for dedupe
  Notify->>Control: load platform.webhook_subscriptions
  Notify->>Control: upsert notification.webhook_deliveries
  Notify->>Control: mark inbox event processed

  Note over Notify,Endpoint: source merchant is always targeted<br/>transfer events can also target destination merchant
  Notify->>Endpoint: POST signed webhook payload

  alt 2xx response
    Endpoint-->>Notify: success
    Notify->>Control: mark delivery SUCCESS
  else 4xx or 5xx or network error
    Endpoint-->>Notify: failure or timeout
    Notify->>Control: keep delivery PENDING or mark FAILED at max attempts
    Note over Notify,Control: retry job scans next_attempt_at and replays pending deliveries
  end
```

## 6. Status And History Read Paths

```mermaid
sequenceDiagram
  autonumber
  actor Client as "Merchant / caller"
  participant Payment as "payment-service"
  participant Transfer as "transfer-service"
  participant Router as "shard-routing"
  participant Control as "PostgreSQL control"
  participant PayDb as "PostgreSQL shard N (payment.*)"

  alt Payment status or payment history
    Client->>Payment: GET /payments/{id} or /payments/{id}/history
    Payment->>Router: locate shard for merchantId
    Router->>Control: SELECT shard_id
    Control-->>Router: shard N
    Payment->>PayDb: read payment row<br/>read payment_events and refund_events
    PayDb-->>Payment: current state and ordered history
    Payment-->>Client: strongly consistent shard-local view
  else Transfer status or transfer history
    Client->>Transfer: GET /transfers/{id} or /transfers/{id}/history
    Transfer->>Control: read transfer.transfers and transfer.transfer_events
    Control-->>Transfer: current state and ordered history
    Transfer-->>Client: control-DB view of saga progress
  end
```
