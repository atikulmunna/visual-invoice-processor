# Visual Invoice Processor

Starter implementation for the Visual Invoice & Receipt Processor system design.

## Implemented in this scaffold

- Typed environment config loading and validation
- Google authentication helpers (service account and OAuth)
- Google Drive client with inbox listing and file download methods
- Extraction service abstraction with OpenAI/Gemini adapters
- Strict JSON parsing with one corrective retry on invalid model output
- Google Sheets storage writer returning append row references
- MVP duplicate skip guard based on in-process `file_hash` tracking
- Configurable ledger backend (`sheets` or `postgres`)
- Baseline schema and validation wiring
- Explicit processing state machine with transition validation
- Durable SQLite claim store for idempotency (`drive_file_id + file_hash`)
- Shared retry utility with exponential backoff and jitter
- Dead-letter JSONL store for terminal failures and replay queries
- Document-correlated JSON logging helper (`document_id`, stage, latency, outcome)
- Simple metrics collector plus JSONL metrics sink for dashboard ingestion
- Review queue router with reason-coded records and `Needs_Review` file moves
- Business-rule validator for totals and line-item consistency with scored output
- Replay tooling with dead-letter scanning and audit trail entries
- Monitoring API endpoints for health, stats, failures, and backlog
- Unit tests for config and Drive MIME filtering
- Optional integration test for Drive listing

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and update values.
4. Ensure Google credentials files exist at configured paths.

## Run

```powershell
python -m app.main poll-once
```

```powershell
python -m app.main replay --status FAILED
```

```powershell
python -m app.monitoring_main
```

## Test

```powershell
pytest -q
```

Integration test (optional):

```powershell
$env:RUN_DRIVE_INTEGRATION_TESTS="1"
pytest -q -m integration
```

## Notes

- For production, prefer service account auth where possible.
- Keep only one processor runtime active (worker or scheduled job).
- Duplicate protection in this phase is process-local only; durable idempotency is planned in Phase 2 (`P2-02`).
