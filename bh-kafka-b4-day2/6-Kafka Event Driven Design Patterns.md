# Event-Driven Architecture Design Patterns with Kafka

This training module covers the essential architectural design patterns used when building production-grade, event-driven microservices with Apache Kafka. It is designed to be **trainer-friendly**, containing learning objectives, architectural diagrams, concrete real-world scenarios, and dedicated discussion prompts for the classroom.

---

## Learning Objectives
By the end of this module, students should be able to:
1.  Explain the **Transactional Outbox** pattern and how it solves the dual-write problem.
2.  Differentiate between **Choreographed** and **Orchestrated Sagas** in distributed transactions.
3.  Design resilient error-handling pipelines using **Retry Queues** and **Dead Letter Queues (DLQs)**.
4.  Implement **Idempotency** in consumers to handle Kafka's "at-least-once" delivery guarantee.
5.  Explain how **Event Sourcing** and **CQRS** separate write performance from query capability.

---

## Pattern 1: The Transactional Outbox Pattern
*Solving the "Dual-Write" Problem.*

### The Problem
When a microservice updates its local database *and* publishes an event to Kafka, it performs a **dual write**. <mark>**In distributed systems, a dual write is highly prone to partial failures:**</mark>
*   If the database write succeeds but publishing to Kafka fails, downstream services never learn of the change (data inconsistency).
*   If publishing to Kafka succeeds but the database write fails (or rolls back), downstream services process phantom events that do not exist in the source of truth database.

### The Solution
<mark>**The Transactional Outbox pattern guarantees at-least-once event delivery by converting the dual-write into a single local database transaction.**</mark> 

1.  Along with business data tables, the service maintains an `outbox` table in the same database.
2.  When a business operation occurs, the service writes the business record *and* a corresponding event record into the `outbox` table inside a **single database transaction**.
3.  A separate agent (like a **Change Data Capture (CDC)** tool, e.g., Debezium) tails the database transaction log, extracts records from the `outbox` table, and publishes them to Kafka.
4.  Once successfully sent to Kafka, the outbox record is marked as processed or deleted.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       ORDER MICROSERVICE                                │
│                                                                         │
│  ┌───────────────────────┐                                              │
│  │    Business Logic     │                                              │
│  └───────────┬───────────┘                                              │
│              │ Start Transaction                                        │
│              ▼                                                          │
│     ┌────────────────────────────────────────────────────────┐          │
│     │ DB TRANSACTION                                         │          │
│     │                                                        │          │
│     │ 1. Write to [Orders Table]   (Insert order state)      │          │
│     │ 2. Write to [Outbox Table]   (Insert "OrderCreated")   │          │
│     │                                                        │          │
│     │ Commit Transaction                                     │          │
│     └────────────────────────┬───────────────────────────────┘          │
└──────────────────────────────┼──────────────────────────────────────────┘
                               │ Database Log
                               ▼
                    ┌─────────────────────┐
                    │ Change Data Capture │ (e.g. Debezium / Log Tailer)
                    │ (CDC) Engine        │
                    └──────────┬──────────┘
                               │ Publish Event
                               ▼
                    ┌─────────────────────┐
                    │    Kafka Topic      │
                    │   "order-events"    │
                    └─────────────────────┘
```

> [!NOTE]
> **Trainer Notes & Discussion Prompt:**
> Ask the class: *"Why is using a polling thread that runs `SELECT * FROM outbox` every second generally inferior to a Change Data Capture (CDC) tool like Debezium?"*
> *   *Key Answer Points:* Polling creates query overhead on the database, introduces lag, and makes it hard to scale the polling process without race conditions. CDC reads the low-level database WAL (Write-Ahead Log) asynchronously, causing near-zero database performance impact.

---

## Pattern 2: The Saga Pattern
*Distributed Transactions without Two-Phase Commit (2PC).*

### The Problem
In a microservice architecture, a single business transaction (e.g., ordering an item) span multiple databases owned by different services (Order Service, Payment Service, Inventory Service). Traditional database-level distributed transactions (2PC) do not scale well, block resources, and create tight runtime coupling.

### The Solution
A **Saga** is a sequence of local transactions. Each local transaction updates the database of a single service and publishes an event. If a step fails, the Saga executes **compensating transactions** (rollback operations) in reverse order to return the system to a consistent state.

There are two styles of Sagas: **Choreography** and **Orchestration**.

---

### Style A: Choreography (Event-Driven Collaboration)
Services listen to Kafka topics, perform their local action, and publish new events that trigger the next service. There is no central controller.

```
┌──────────────┐             ┌──────────────┐             ┌──────────────┐
│ Order        ├─(Created)──►│ Payment      ├─(Charged)──►│ Inventory    │
│ Service      │             │ Service      │             │ Service      │
└──────────────┘             └──────────────┘             └──────────────┘
```

*   **Pros:** Highly decoupled; simple to understand for small workflows; no single point of failure.
*   **Cons:** Hard to track the state of a complex workflow; risk of cyclic dependencies; debugging can become extremely difficult.

---

### Style B: Orchestration (Central Controller)
A dedicated coordinator service (Orchestrator) manages the state machine of the Saga. It publishes command messages to Kafka, waits for replies, and decides the next step.

```
                       ┌──────────────────────┐
                       │   Saga Orchestrator  │◄───(Tracks State)
                       └────┬────────────▲────┘
                 1. Command │            │ 2. Reply (Success/Fail)
                            ▼            │
                ┌────────────────────────┴────────┐
                │ Kafka Topics (Requests & Replies)│
                └───────────┬────────────▲────────┘
                            │            │
                            ▼            │
                       ┌─────────────────┴────┐
                       │ Payment / Inventory  │ (Local Actions)
                       │ Microservices        │
                       └──────────────────────┘
```

*   **Pros:** Centralized visibility; workflow state is easily queried; prevents cyclic dependencies.
*   **Cons:** Orchestrator contains complex business logic; risk of orchestrator becoming a bottleneck or a single point of failure if not scaled correctly.

> [!IMPORTANT]
> **Saga Compensating Action Example:**
> If `Inventory Service` reports *out of stock*, the orchestrator publishes a `RefundPayment` command. The `Payment Service` consumes this and reverses the payment. <mark>**Sagas guarantee eventual consistency, not immediate ACID consistency.**</mark>

---

## Pattern 3: Dead Letter Queue (DLQ) & Retry Queues
*Resilient and Non-Blocking Error Handling.*

### The Problem
<mark>**When a consumer encounters an error while processing a message (e.g., database timeout or a corrupted payload), simply throwing an exception and retrying in place blocks the partition.**</mark> <mark>**No subsequent messages can be processed, introducing high consumer lag.**</mark>

### The Solution
Implement a multi-tiered error-handling pipeline containing:
1.  **Main Topic:** Where normal traffic is processed.
2.  **Retry Topics (with Backoff):** Where transient errors (e.g., temporary database down) are sent to be retried after a delay.
3.  **Dead Letter Queue (DLQ):** Where non-transient errors (e.g., deserialization failure / "poison pill") are routed for manual inspection.

```
                  ┌─────────────────────┐
                  │     Main Topic      │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │   Order Consumer    │
                  └──────┬───────────┬──┘
                         │           │
           If Poison     │           │ If Transient
           Pill          ▼           ▼ Error
     ┌───────────────────────┐   ┌───────────────────────┐
     │ Dead Letter Queue     │   │      Retry Topic      │
     │ (DLQ / order-err)     │   │ (order-retry-5m)      │
     └───────────────────────┘   └───────────┬───────────┘
                                             │
                                             ▼
                                 (Processed after 5m delay)
```

### Flow Checklist for Production retry
1.  **Fail Fast:** Differentiate between *transient* errors (network timeout, database lock) and *non-transient* errors (JSON parse error, validation failure).
2.  **Poison Pills:** <mark>**Route non-transient errors directly to the DLQ. Do not retry them, as they will fail forever and consume system resources.**</mark>
3.  **Delayed Retries:** Do not block the main thread. Publish transient failures to a retry topic. The retry consumer can use a pausing strategy (`KafkaConsumer.pause()`) to respect retry delays (e.g., wait 5 minutes before reading).

---

## Pattern 4: Idempotent Consumer (Message Deduplication)
*Surviving "At-Least-Once" Delivery.*

### The Problem
Kafka's default delivery guarantee is **at-least-once**. This means a message is guaranteed to reach the consumer, but in failure scenarios (e.g., consumer crashes after processing but before committing offsets), the consumer will receive duplicate messages upon restart. 

If a duplicate message represents a financial transaction (e.g., "Charge \$100"), processing it twice is catastrophic.

### The Solution
<mark>**The consumer must be idempotent—meaning processing the same message multiple times results in the same system state as processing it exactly once.**</mark>

```
                             Duplicate Events Received
                             ┌───────────────────────┐
                             │ "tx-id: 981, val:100" │
                             └──────────┬────────────┘
                                        │
                                        ▼
                             ┌───────────────────────┐
                             │  Idempotency Check   │
                             └──────────┬────────────┘
                                        │
                      Query Cache/DB    ▼    Is Duplicate?
                    ┌──────────────────────────────────┐
                    │  SELECT processed FROM tx_log    │
                    │  WHERE tx_id = '981'             │
                    └───────────────────┬──────────────┘
                                        │
                              ┌─────────┴─────────┐
                           No │               Yes │
                              ▼                   ▼
                     ┌─────────────────┐ ┌─────────────────┐
                     │ Process Event & │ │  Acknowledge    │
                     │ Log: tx_id=981  │ │  and Skip       │
                     └─────────────────┘ └─────────────────┘
```

### Key Techniques
1.  **Unique Business Keys:** Embed a unique identifier (like a UUID or transaction ID) in the event payload headers or body.
2.  **Idempotency Table / Cache:** Maintain a lookup table (e.g., in Redis or SQL) of processed IDs. Check if the ID exists before processing; if it does, discard the message as a duplicate.
3.  **Upsert Operations:** Design database updates using database operations like `UPSERT` (e.g., `INSERT ON CONFLICT UPDATE`) or mathematical assignments (`SET balance = 100` instead of `ADD balance, 100`).

---

## Pattern 5: Event Sourcing & CQRS
*Architecting a Scalable, Replayable Audit Log.*

### Concept
*   **Event Sourcing:** Instead of storing only the *current state* of an entity in a database, store all state changes as an immutable sequence of events (the "Append-Only Log"). The current state is calculated by replaying these events from offset 0.
*   **CQRS (Command Query Responsibility Segregation):** Separates the data model used for updating database state (Commands/Writes) from the model used to read data (Queries/Reads).

```
   COMMAND SIDE                                   QUERY SIDE
   (Write Model)                                 (Read Model)
  ┌──────────────┐                             ┌──────────────┐
  │ Write        │                             │ Query        │
  │ Microservice │                             │ Microservice │
  └──────┬───────┘                             └──────▲───────┘
         │ 1. Validate Command                        │ 4. Read Fast
         ▼                                            │
  ┌──────────────┐                             ┌──────┴───────┐
  │ Local State  │                             │ Read-Only DB │
  │   Store      │                             │ (Elastic /   │
  └──────┬───────┘                             │  Redis)      │
         │ 2. Append Event                     └──────▲───────┘
         ▼                                            │
  ┌──────────────┐                                    │ 3. Project /
  │ Kafka Event  ├────────────────────────────────────┘    Denormalize
  │   Log        │
  └──────────────┘
```

### How They Work Together
1.  A user submits a command (e.g., `RenameUser`).
2.  The Write Service validates the command against its current state, then writes a `UserRenamed` event to Kafka.
3.  Kafka serves as the immutable journal.
4.  A projection worker consumes the `UserRenamed` event and updates a read-optimized database (like Elasticsearch or a denormalized SQL table).
5.  Read queries are routed to the read-optimized database, achieving sub-millisecond query performance.

> [!TIP]
> **Trainer Notes & Discussion Prompt:**
> Ask the class: *"What happens if a bug is deployed to the query service database, corrupting all read data?"*
> *   *Key Answer Points:* <mark>**With Event Sourcing, you can blow away the read database, fix the bug in your projection code, reset your Kafka consumer group offset to 0, and replay the event log to rebuild the read model from scratch.**</mark>

---

## Classroom Exercises & Review

### Scenario Challenge: E-Commerce Inventory Control
**Scenario:** An order is placed on an e-commerce website. The checkout flow requires:
1.  Reserving inventory in the Inventory Service.
2.  Charging the credit card in the Payment Service.
3.  Updating the Order status to `SHIPPED` in the Order Service.

**Exercise:**
*   Divide into groups.
*   Draw a diagram showing how you would implement this using a **Choreographed Saga** versus an **Orchestrated Saga**.
*   Define the failure path: What happens if the payment succeeds, but the inventory check fails? What compensating events must be published, and which services must consume them?
