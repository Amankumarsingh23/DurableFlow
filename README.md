# DurableFlow

A durable workflow orchestration engine built incrementally for learning and SDE interview preparation.

Sessions 5 and 6 establish persistence, workflow-definition registration and atomic run creation. Activities are not executed; workflow execution and crash recovery are not implemented.

## Prerequisites

- JDK 21 (JAVA_HOME and java on PATH).
- Maven 3.9.x (mvn on PATH).
- Docker Engine 24+ or Docker Desktop with Linux containers, and Docker Compose v2.
- A running Docker daemon for both local PostgreSQL and Testcontainers tests.

## Run locally

From the repository root:

```powershell
docker compose up -d --wait
mvn spring-boot:run
```

In another terminal:

```powershell
Invoke-RestMethod http://localhost:8080/actuator/health
```

Expected response: {"status":"UP"}. Flyway applies V1 before Hibernate validates the schema and the HTTP application becomes ready. Health includes the database connection.

Stop the application with Ctrl+C. Stop infrastructure with:

```powershell
docker compose down
```

The named postgres_data volume preserves database data across container restarts. Removing the volume deletes local data.

## Tests

```powershell
mvn verify
```

The integration tests start an isolated PostgreSQL 17.6 Testcontainers database, apply Flyway migrations, start the HTTP application with Hibernate validation, check health, and round-trip all three entities. Docker is required; tests deliberately do not silently skip when Docker is missing.

## Configuration

| Variable | Local default | Purpose |
| --- | --- | --- |
| DB_NAME | durableflow | Database name |
| DB_USERNAME | durableflow | Database user |
| DB_PASSWORD | durableflow_local | Local development password |
| DB_PORT | 5432 | Host PostgreSQL port |
| DB_URL | jdbc:postgresql://localhost:5432/durableflow | Optional full JDBC URL override |

For overrides, set environment variables in the terminal before running both Compose and Maven. For example:

```powershell
$env:DB_PORT = "5433"
$env:DB_PASSWORD = "my-local-password"
docker compose up -d --wait
mvn spring-boot:run
```

Compose also reads .env, but Spring does not automatically read that file. Export matching variables when using Maven. PostgreSQL initialization variables apply only to a fresh volume; changing credentials does not update an existing database.

## Files and responsibilities

- pom.xml: Java 21, Spring Boot 4.1.2, framework-managed dependency versions.
- docker-compose.yml: PostgreSQL only, localhost port binding, health check and named volume.
- application.yml: environment configuration, Flyway and ddl-auto=validate.
- V1__create_workflow_tables.sql: authoritative schema with keys and constraints.
- workflow/definition, workflow/run, workflow/activity: JPA entities, enums and CRUD repositories.
- PersistenceIntegrationTest: migration, startup, health and PostgreSQL persistence checks.

## Persistence decisions

workflow_definition stores a versioned JSON recipe, unique by (name, version), with positive versions.
workflow_run references one definition and stores input and lifecycle timestamps.
activity_execution references one run, unique by (workflow_run_id, activity_id), and stores nullable input/output, lifecycle timestamps and a nonnegative attempt counter.

IDs use generated UUIDs; timestamps use Instant/TIMESTAMPTZ. Callers explicitly supply timestamps; lifecycle behavior will be introduced in later sessions. JSONB uses Hibernate @JdbcTypeCode(SqlTypes.JSON) with Map<String, Object> for portable JSON values. Status enums use string mappings and matching database CHECK constraints.

Foreign keys are mapped as UUID fields rather than ORM associations, keeping persistence simple and avoiding implicit graph loading. The database enforces references. There are no cascading deletes.

Flyway's Boot starter enables migration auto-configuration; flyway-database-postgresql supplies PostgreSQL support. Hibernate only validates and never creates or updates tables. Once applied, migrations should remain unchanged; evolve the schema with a new numbered migration.

## Session 5 verification status

On the authoring machine, Java, Maven and Docker are unavailable on PATH. Compilation, integration tests and application startup remain unverified for Sessions 5 and 6. Run the commands above after installing the prerequisites. XML/JSON parsing and Git whitespace checks are the available static checks.

## Session 6 API examples

Start PostgreSQL and the application using the commands above. Run these commands from the repository root in PowerShell. Use curl.exe to avoid the Windows PowerShell curl alias (on Linux/macOS, use curl instead).

Register the immutable OrderFulfillment v1 recipe (201 Created):

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-definitions -H "Content-Type: application/json" --data-binary "@examples/order-fulfillment-v1.json"
```

Read the recipe:

```powershell
curl.exe http://localhost:8080/api/workflow-definitions/OrderFulfillment/versions/1
```

Start a run (201 Created):

```powershell
curl.exe -i -X POST http://localhost:8080/api/workflow-runs -H "Content-Type: application/json" --data-binary "@examples/start-order.json"
```

Copy runId from the response, then inspect persisted state:

```powershell
$runId = "replace-with-returned-runId"
curl.exe "http://localhost:8080/api/workflow-runs/$runId"
```

The run is RUNNING with a startedAt timestamp in the database. All three logical activities are PENDING with attempt_count zero and no execution timestamps. Session 6 does not execute activities or determine eligibility. PENDING means unfinished, and does not mean dependencies are satisfied.

POST and GET responses use DTOs and never expose JPA entities. Run responses include runId, workflowName, version, status, input and activity states (activityId and status). GET lists activities by logical ID for deterministic output; this ordering is not an execution plan.

Definition registration trims names, step IDs, activity names and dependency references, defaults omitted/null dependsOn to an empty list, and removes repeated dependency references. Typed Bean Validation checks required fields and database length limits. The service rejects duplicate step IDs, unknown/self dependencies and cycles using Kahn's algorithm in O(V + E) time. No recursion is needed. Name and logical IDs are case sensitive.

Definitions are immutable through the API: there is no update or delete operation. Register a new version to change a recipe. Duplicate (name, version) is rejected by both the service and the database unique constraint, including concurrent registrations.

WorkflowRunService.start is the transaction boundary: it resolves the stored recipe, creates the RUNNING run, and inserts exactly one PENDING activity per step. All inserts commit together or roll back together. Activity input/output remain null until a future execution session. The run input must be a JSON object (use {} when empty).

Errors consistently use {"status":400,"message":"..."}. Invalid requests/graphs return 400, missing definitions/runs return 404, duplicate definitions or persistence conflicts return 409, and unexpected failures return a generic 500. Server details are not returned to clients.

### Session 6 tests

```powershell
mvn -Dtest=WorkflowDefinitionValidatorTest test
mvn verify
```

The validator unit tests cover sequential/branching graphs, duplicate IDs, missing/self dependencies, multi-node/disconnected cycles, normalization and invalid fields. They need Java/Maven but not Docker.

WorkflowApiIntegrationTest uses real PostgreSQL and HTTP to check definition registration/retrieval/duplicates, input errors, run retrieval and initial row states. A test-only PostgreSQL trigger forces an activity insert failure and verifies that neither the run nor any activity remains after rollback. The trigger is removed in finally and is never part of production migrations.

No Session 6 schema changes were needed; V1 remains unchanged. No workers, queues, execution engine or external infrastructure were added.
