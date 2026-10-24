# DurableFlow

A durable workflow orchestration engine built incrementally for learning and SDE interview preparation.

Session 5 establishes persistence only. Workflow execution and crash recovery are not implemented.

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

Implementation is present. On the authoring machine, Java, Maven and Docker were unavailable on PATH, so compilation, integration tests and application startup have not been verified. Run the commands above after installing the prerequisites. Do not treat this scaffold as a verified running engine.
