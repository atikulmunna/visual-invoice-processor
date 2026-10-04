# Ledgerly Invoice Processor

**Turn invoices and receipts into checked, reviewable records, with each organization's data kept separate.**

[![Open live demo](https://img.shields.io/badge/OPEN_LIVE_DEMO-3B5BFF?style=for-the-badge&logoColor=white)](https://2wuikcntsyyqfgkucq5m26vyfe0kekgl.lambda-url.ap-southeast-1.on.aws/)

---

Ledgerly is a private-alpha invoice processing application for a small, controlled group of testers. Users upload PDF, PNG, or JPEG documents directly to a private Amazon S3 bucket. An event-driven worker extracts the fields with a low-cost model through OpenRouter, normalizes and validates them, and either stores a structured record in Supabase Postgres or sends the document to a human review queue. Every document, record, and review item belongs to an organization, and no organization can see another's data.

## Product preview

<div align="center">
  <a href="https://2wuikcntsyyqfgkucq5m26vyfe0kekgl.lambda-url.ap-southeast-1.on.aws/">
    <img src="assets/dashboard.png" alt="Ledgerly invoice processing dashboard" width="100%" />
  </a>
  <sub>Private-alpha workspace for direct S3 uploads, extraction results, records, and review operations.</sub>
</div>

## Highlights

- **Separate workspaces:** every job, record, review item, and duplicate check is scoped to an organization, and isolation is tested against a real Postgres database before every deploy.
- **Honest extraction:** a missing vendor or invoice date is never guessed. The document goes to review with a plain reason, and approval stays blocked until a person fills the gap.
- **Low-cost AI:** extraction runs through OpenRouter on Gemini 2.5 Flash Lite at roughly USD 0.25 per 1,000 single-page documents, with hard caps on every call.
- **Direct uploads:** the browser uploads straight to private S3 with a five-minute presigned policy; document bytes never pass through the web function.
- **Batch uploads:** drop many files at once and follow each one through upload, extraction, and validation, with a plain reason for anything rejected and retry for failures.
- **Validation and review:** schema checks, arithmetic checks on totals and line items, confidence scoring, and a review queue with approve, reject, and duplicate actions.
- **Safe reprocessing:** idempotent jobs and duplicate-safe writes; the same file uploaded twice in one organization is caught, while two organizations can process identical files independently.
- **Operations on a budget:** infrastructure as code, least-privilege IAM, secrets in SSM, seven-day log retention, and an annual AWS budget with alerts.

## Architecture

```mermaid
flowchart LR
    USER[Signed-in tester] -->|Request upload authorization| WEB[FastAPI web Lambda]
    WEB -->|Five-minute policy| USER
    USER -->|Direct upload| INBOX[(Private S3 inbox)]
    INBOX -->|Object-created event| WORKER[Processing Lambda]
    WORKER --> EXTRACT[OpenRouter extraction]
    EXTRACT --> NORMALIZE[Normalize and validate]
    NORMALIZE -->|Complete and consistent| DB[(Supabase Postgres)]
    NORMALIZE -->|Missing or doubtful| REVIEW[Review queue]
    WORKER --> ARCHIVE[(S3 archive)]
    DB --> WEB
    REVIEW --> WEB
```

Every read and write is bounded by the signed-in session's organization. The web function never sees document bytes and cannot read the model key; only the processing function can.

## Processing lifecycle

1. A tester signs in; the session carries their active organization.
2. The API checks the tester's upload allowance, creates a processing job owned by the organization, and returns a presigned S3 policy.
3. The browser uploads the file directly to the private `inbox/` prefix.
4. S3 invokes the worker, which claims the job and reads the organization's base currency.
5. The worker verifies the file size, signature, declared type, and PDF page count, and skips files this organization has already processed.
6. The model extracts the fields; normalization cleans them up and recovers values from the PDF text layer where the model missed them.
7. Validation checks the schema and the arithmetic. Complete, consistent records are stored; anything missing or doubtful goes to the review queue with a reason for each problem.
8. The source file moves to `archive/`, and the workspace shows the result.

## Extraction

Extraction uses one provider, OpenRouter, with `google/gemini-2.5-flash-lite` as the default model. It reads images and scanned PDFs natively and spends no hidden reasoning tokens. Cost controls are built in:

- Every reply is capped at 2,000 tokens (`OPENROUTER_MAX_TOKENS`).
- PDFs are read by the model itself, never by OpenRouter's paid OCR engine.
- Malformed JSON is repaired locally before any retry, and at most one retry is made.
- Token usage and cost are logged for every call.

Set a credit limit on the OpenRouter key itself as a final guard.

### Values are never invented

| Situation | Result |
| --- | --- |
| Vendor name not on the document | Field left empty; review reason `missing_vendor` |
| Invoice date not on the document | Field left empty; review reason `missing_invoice_date` |
| Totals or line items do not add up | Review reason `validation_failed` |
| Low model confidence | Review reason `low_confidence` |
| Currency not readable | Organization base currency is used and the record is flagged `currency_assumed` |

Currency is resolved from the model's answer first, then from the dominant currency marker in the document text (`$`, `৳`, `Tk`, `USD`, and others), and only then from the organization's base currency, which is BDT unless changed. Approval of a review item is refused until every required field is filled in.

## Technology

| Layer | Technology | Responsibility |
| --- | --- | --- |
| Web application | FastAPI, Mangum, AWS Lambda | Sign-in, workspace API, upload authorization, review actions |
| Workspace frontend | React, TypeScript, Vite | Single-page workspace served at `/app` |
| Object storage | Amazon S3 | Private inbox, event trigger, short-lived archive |
| Processing | Python 3.13, AWS Lambda | File inspection, extraction pipeline, validation |
| AI extraction | OpenRouter (Gemini 2.5 Flash Lite) | Invoice and receipt field extraction |
| Database | Supabase Postgres | Organizations, users, sessions, jobs, records, reviews |
| Infrastructure | AWS SAM, CloudFormation | Repeatable serverless provisioning |
| Delivery | GitHub Actions, GitHub OIDC | Tests, image build, deployment, health check |
| Quality | Pytest, Vitest, Ruff, golden datasets | Regression, isolation, and extraction-quality checks |

## Private-alpha guardrails

| Control | Current limit |
| --- | ---: |
| Active testers | 10 |
| Documents per tester | 20 |
| Maximum upload size | 5 MB |
| Maximum PDF length | 5 pages |
| Global page-processing attempts | 1,000 |
| Presigned upload lifetime | 5 minutes |
| Abandoned inbox retention | 1 day |
| Archived document retention | 30 days |
| Lambda log retention | 7 days |

The S3 bucket blocks public access, enforces TLS, and encrypts objects at rest. Production secrets are loaded from AWS Systems Manager Parameter Store.

## Project structure

```text
app/
  monitoring_api.py        FastAPI app, authentication, and review and upload API
  workspace_api.py         Sign-in, account, and organization API; serves /app
  web_session.py           Session cookie settings
  serverless_worker.py     S3-event processing worker
  extraction_service.py    OpenRouter extraction and PDF text enrichment
  normalization_engine.py  Field normalization, recovery, and currency resolution
  validation.py            Schema checks, business rules, and review reasons
  review_queue.py          Organization-scoped review queue
  alpha_store.py           Organizations, users, sessions, quotas, jobs, and claims
  alpha_admin.py           Administrator command line
  db_migrate.py            Ordered migration runner
frontend/                   Workspace single-page app (React, TypeScript, Vite)
assets/                     Product icon and dashboard screenshot
config/                     Data-driven normalization rules
eval/                       Standard and strict golden datasets
infra/                      GitHub OIDC bootstrap stack
migrations/                 Ordered Supabase/Postgres migrations
schemas/                    Invoice data model
tests/                      Unit, API, isolation, storage, and worker tests
template.yaml               AWS SAM application
Dockerfile                  Lambda container image, including the frontend build
```

## Local development

### Requirements

- Python 3.13 and Node.js 24
- An AWS account and private S3 bucket
- A Supabase Postgres project
- An OpenRouter API key

### Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
cd frontend; npm install; npm run build; cd ..
```

Configure `.env` with the required values:

```dotenv
INGESTION_BACKEND=s3
S3_BUCKET_NAME=your-private-bucket
S3_REGION=ap-southeast-1

LEDGER_BACKEND=postgres
POSTGRES_DSN=postgresql://...
ALPHA_AUTH_ENABLED=true

OPENROUTER_API_KEY=...
```

Apply the ordered database migrations, then start the application:

```powershell
python -m app.db_migrate
python -m app.monitoring_main
```

Sign in at `http://localhost:8000/login`.

### Workspace frontend

The workspace in `frontend/` is built with Vite and served by FastAPI under `/app`. Sign-in lands in the workspace. Screens not yet rebuilt there link to the classic dashboard at `/dashboard`, which stays available until the new screens reach parity.

```powershell
cd frontend
npm run build   # FastAPI serves frontend/dist at http://localhost:8000/app
npm run dev     # or develop with hot reload at http://localhost:5173/app
npm test
```

The dev server proxies the API and sign-in routes to FastAPI on port 8000, so sign in at `http://localhost:5173/login`.

## Database migrations

Migrations live in `migrations/` and are applied in order by `python -m app.db_migrate`, which records each one in `schema_migrations`. The deploy workflow does not run them. Before pushing code that needs a new migration, apply it to production first, using the connection string stored in SSM:

```powershell
$env:POSTGRES_DSN = aws ssm get-parameter --name /invoice-processor/alpha/postgres-dsn --with-decryption --region ap-southeast-1 --query Parameter.Value --output text
python -m app.db_migrate
```

Migrations are written so the code already in production keeps working against the new schema.

## Tester administration

Run administrator commands only from a trusted machine with `POSTGRES_DSN` set to the production database, as shown above.

```powershell
python -m app.alpha_admin create --username tester-one
python -m app.alpha_admin list
python -m app.alpha_admin set-password --username tester-one
python -m app.alpha_admin reset --username tester-one
python -m app.alpha_admin disable --username tester-one
python -m app.alpha_admin enable --username tester-one
```

`create` and `set-password` generate a password and show it once; `set-password` also signs the tester out everywhere. `reset` restores a tester's upload allowance. Never commit passwords or store them in deployment variables.

## Organizations

Each tester gets a personal organization when the account is created, and every request is scoped to the organization of the signed-in session. Owners can change their organization's base currency in the workspace settings; members see it read-only.

```powershell
python -m app.alpha_admin org-create --name "Acme Traders" --owner tester-one
python -m app.alpha_admin org-add-member --org-id <org-id> --username tester-two --role member
python -m app.alpha_admin org-remove-member --org-id <org-id> --username tester-two
python -m app.alpha_admin org-list
python -m app.alpha_admin org-set-currency --org-id <org-id> --currency USD
```

Records created before organizations existed that cannot be traced to an upload stay unowned and hidden. To assign them to an organization:

```powershell
python -m app.alpha_admin org-adopt-unowned --org-id <org-id>
```

## Testing

```powershell
pytest -q
ruff check app tests
cd frontend; npm test; npm run typecheck
```

The isolation suite runs against a real PostgreSQL server when `TEST_POSTGRES_DSN` points at one whose role can create databases; each run creates and drops its own database. CI runs it against a Postgres service container before every deploy.

```powershell
$env:TEST_POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
pytest -q tests/test_org_isolation_postgres.py
```

Extraction quality is measured against golden datasets. Each run calls the model once per document, so it spends a little OpenRouter credit.

```powershell
python -m app.evaluation --dataset eval/golden_set.json --fail-under 0.90
python -m app.evaluation --dataset eval/golden_set_strict.json --fail-under 0.75 --output logs/golden_eval_strict_report.json
```

## Deployment

The application is declared in `template.yaml` and deployed through `.github/workflows/deploy-aws.yml` using GitHub OIDC, so no long-lived AWS keys are stored in the repository.

The stack provisions:

- A web Lambda with a public Function URL and application-level authentication
- An S3-triggered processing Lambda, the only function allowed to read the OpenRouter key
- A private encrypted S3 bucket with lifecycle rules
- Two CloudWatch log groups with seven-day retention
- An annual AWS cost budget with threshold and forecast alerts

Follow [AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md) for the one-time AWS, SSM, Supabase, IAM, and repository setup.

## Continuous integration

| Workflow | Purpose |
| --- | --- |
| `deploy-aws.yml` | Runs Python tests with a Postgres service, frontend tests and type check, validates SAM, builds the Lambda images, deploys, and checks `/health` |
| `golden-eval.yml` | Runs the standard extraction-quality gate when application, config, or eval files change |
| `golden-eval-strict-nightly.yml` | Runs the strict benchmark weekly |
| `supabase-keepalive.yml` | Pings the database every three days so the free-plan project stays awake |

## Security

- Every query and every review action is limited to the signed-in organization; requests that touch another organization's jobs or review items are refused.
- Passwords are hashed with scrypt, sign-in input lengths are capped, and sessions are opaque server-side tokens stored as hashes.
- Session cookies are secure, HTTP-only, and use `SameSite=Lax`.
- After sign-in, only same-site return paths are honored, so a crafted link cannot redirect anyone off the site.
- Production secrets are stored as SSM `SecureString` parameters, and only the processing Lambda can read the model key.
- Supabase browser roles have no direct access to operational tables.
- The web Lambda can authorize inbox uploads but cannot process archive objects.
- Never commit `.env`, tester credentials, database passwords, or model keys.

## Roadmap

Planned features, roughly in order. The product is heading two ways at once: VAT-ready books for small businesses in Bangladesh, and control over bills before they are paid for teams anywhere.

**Workspace**

- A review workspace with the document beside an editable form, live arithmetic checks, and keyboard shortcuts
- Records with filters, search, a detail view, and CSV and Excel export
- An overview of spending trends, top vendors, and the review backlog

**Smarter extraction**

- A vendor master that merges spelling variants and keeps each vendor's history
- Duplicate detection across different files of the same invoice, not only identical bytes
- Field-level confidence based on whether each value actually appears in the document
- Vendor memory: reviewer corrections teach extraction, so repeat vendors are approved automatically

**VAT-ready books for Bangladesh**

- Recognition of Mushak-6.3 VAT challans, supplier BINs, and VAT amounts, with flags on bills that cannot support an input VAT claim
- A monthly purchase book (Mushak-6.1) and an input VAT summary for the Mushak-9.1 return, exported in the layout consultants use
- VAT deducted at source tracking with Mushak-6.6 certificates
- Matching bKash and Nagad payment receipts to the bills they paid
- Better reading of Bangla text and handwritten cash memos

**Control over bills before they are paid**

- Due dates, payment status, and a calendar of upcoming payments
- Alerts when a vendor's bank details change
- Alerts when a vendor's unit prices creep up

**Getting documents in and data out**

- An email forwarding address for each organization
- WhatsApp intake
- An accountant workspace for managing many client organizations
- Exports for Tally, QuickBooks, and Xero
- Self-serve sign-up with plans

---

<div align="center">
  Built as a cost-controlled private alpha on AWS.
</div>
