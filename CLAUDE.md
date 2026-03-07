# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**open-race-telemetry** — open-source, local-first sim racing telemetry pipeline. Captures UDP telemetry from EA F1 25, pipes it through Kafka into TimescaleDB, and visualises it in Grafana. This repo is the data infrastructure layer that will eventually power the **AI Race Engineer** desktop app (separate repo, post-MVP).

Full requirements: `docs/mvp.md`

## Tech Stack

- **.NET 10** / **C# 14** — target `net10.0`, `<LangVersion>14</LangVersion>`
- **F1Game.UDP** v25.1.0 (NuGet) — packet decoding. Do NOT write a custom UDP parser.
- **Confluent.Kafka** — message bus producer/consumer
- **Npgsql** — TimescaleDB writes (use COPY binary protocol for batch inserts)
- **Serilog** — structured logging
- **xUnit + Testcontainers** — testing
- **Docker Compose** — Kafka (KRaft), TimescaleDB (PG16), Grafana

## Commands

```bash
# Infrastructure
cd infra && docker compose up -d
cd infra && docker compose down

# Build
dotnet build src/TelemetryIngester

# Run
dotnet run --project src/TelemetryIngester

# Test (all)
dotnet test src/TelemetryIngester.Tests

# Test (single test by name)
dotnet test src/TelemetryIngester.Tests --filter "FullyQualifiedName~TestMethodName"

# Test (single class)
dotnet test src/TelemetryIngester.Tests --filter "ClassName~PacketMapperTests"

# Format / lint
dotnet format src/TelemetryIngester
```

## Project Structure

```
src/TelemetryIngester/          # Single .NET 10 worker service (the only .NET project)
  Services/                     # BackgroundServices (UdpListenerService, KafkaConsumerService)
  Mapping/                      # PacketMapper: F1Game.UDP structs → canonical event DTOs
  Events/                       # Canonical event record types (CarTelemetryEvent, LapDataEvent, etc.)
  Kafka/                        # KafkaProducer wrapper
  Storage/                      # TimescaleWriter (batch insert logic)
src/TelemetryIngester.Tests/    # xUnit tests
infra/                          # docker-compose.yml, TimescaleDB init.sql, Grafana provisioning
docs/                           # Architecture docs, MVP spec
```

There is ONE .NET project. Do not create additional class library projects. The UDP listener, Kafka producer, Kafka consumer, and TimescaleDB writer all live in `TelemetryIngester` as separate services registered in the same host.

## Architecture

Data flows: **UDP (F1 25 game)** → `UdpListenerService` → **Kafka** → `KafkaConsumerService` → **TimescaleDB** → **Grafana**

- `UdpListenerService` receives raw UDP bytes, calls `byte[].ToPacket()` (F1Game.UDP), passes the decoded packet to `PacketMapper`, then publishes resulting events to Kafka topics.
- `KafkaConsumerService` reads from Kafka, buffers events, and batch-flushes to TimescaleDB via `TimescaleWriter` using Npgsql COPY binary protocol.
- `PacketMapper` is the **only** place that touches F1Game.UDP types. Everything downstream uses canonical event records.
- Each canonical event record has: `EventType`, `SessionUid`, `Timestamp`, `FrameId`, `CarIndex`, plus domain-specific fields.
- By default, only the player car is emitted (`PlayerCarIndex` from packet header). All 20 cars are emitted when `Telemetry:AllCars = true`.

**Kafka message format:**
```json
{
  "eventType": "CarTelemetry",
  "sessionUid": "...",
  "timestamp": "...",
  "frameId": 12345,
  "carIndex": 0,
  "data": { ... }
}
```

**TimescaleDB:** one hypertable per telemetry domain, `TIMESTAMPTZ` time column, individual columns for tyre temps (not arrays/JSON).

## Coding Conventions

- Use C# 14 features: `field` keyword, pattern matching `switch` expressions, null-conditional assignment
- Records for DTOs, classes for services
- Constructor injection via `Microsoft.Extensions.DependencyInjection`
- `IOptions<T>` pattern for configuration sections
- Async all the way — no `.Result` or `.Wait()`
- `CancellationToken` propagated through all async methods
- Logging: `Information` for lifecycle events (startup, shutdown, session detected), `Debug` for per-packet tracing. Never log at `Debug` by default.
- No `Console.WriteLine` — use `ILogger<T>` everywhere

## Configuration

All config via `appsettings.json` / `appsettings.Development.json`. Sections:

- `Telemetry` — `ListenPort` (default 20777), `AllCars` (default false)
- `Kafka` — `BootstrapServers`, `GroupId`
- `TimescaleDb` — `ConnectionString`
- `Ingester` — `BatchSize` (default 100), `FlushIntervalMs` (default 500)

`appsettings.Development.json` must have working local defaults (localhost ports, telemetry/telemetry creds) so `dotnet run` works with zero config after `docker compose up -d`.

## Testing

- **Unit tests** for `PacketMapper` — assert field mapping, player-car filtering, all-cars mode
- **Unit tests** for Kafka serialisation — JSON round-trip of event DTOs
- **Integration tests** with Testcontainers for `TimescaleWriter` and end-to-end (UDP → DB)
- Do NOT test F1Game.UDP's decoding — that's the library's responsibility

## Git Workflow

For every new feature or piece of work:

1. Create a feature branch from `main` with a descriptive name: `git checkout -b feat/short-description`
2. Commit changes to the feature branch as work progresses
3. When the work is complete, open a pull request using the `gh` CLI:
   ```bash
   gh pr create --base main --title "Short description" --body "Summary of changes"
   ```
4. Do NOT merge the PR — leave it for the user to review and merge

## Do NOT

- Write a custom UDP packet parser or binary deserialiser
- Create separate class library projects — keep it as one project
- Use `MemoryMarshal` or `Span<byte>` for packet parsing (F1Game.UDP handles this)
- Add AI, MCP, voice, or cloud sync features — those belong in the `ai-race-engineer` repo
- Target F1 24 or earlier — MVP is F1 25 only
- Use `System.Text.Json` source generators unless there's a measurable perf need
