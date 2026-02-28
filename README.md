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
- Baseline schema and validation wiring
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
