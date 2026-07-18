# AWS Private-Alpha Deployment

The AWS deployment is serverless: FastAPI runs behind a Lambda Function URL, browsers upload directly to private S3 with a five-minute presigned policy, and an S3 event invokes the processing Lambda. Supabase remains the durable database and Mistral remains the only extraction provider.

## Cost and safety boundaries

- AWS annual budget: USD 15, with 50%, 80%, 100%, and forecast alerts.
- Mistral workspace spending limit: set manually to USD 10.
- Maximum 10 active testers, 20 authorized documents each, 5 MB and 5 pages per document.
- Global maximum of 1,000 page-processing attempts.
- S3 deletes abandoned inbox objects after one day and archives after 30 days.
- Lambda concurrency is capped at two for both web and worker functions.
- No VPC, NAT Gateway, database, load balancer, custom domain, or provisioned concurrency is created.

## One-time setup

1. Configure `POSTGRES_DSN`, then apply all ordered migrations with
   `python -m app.db_migrate`. This creates the base tables, analytics views,
   private-alpha state, and browser-role access restrictions.
2. Create Standard `SecureString` parameters in `ap-southeast-1` for the Supabase DSN and Mistral API key. Use the AWS-managed SSM key, not a customer-managed monthly KMS key.
3. Bootstrap GitHub OIDC once. This creates a repository-specific deployment role,
   a least-privilege CloudFormation execution role, two ECR repositories, and a
   seven-day deployment-artifact bucket:

   ```powershell
   aws cloudformation deploy `
     --template-file infra/github-oidc-role.yaml `
     --stack-name invoice-processor-github-oidc `
     --capabilities CAPABILITY_NAMED_IAM `
     --parameter-overrides GitHubOwner=<owner> GitHubRepository=<repository> `
     --region ap-southeast-1
   ```

   If the account already has the GitHub Actions OIDC provider, add
   `ExistingGitHubOidcProviderArn=arn:aws:iam::<account-id>:oidc-provider/token.actions.githubusercontent.com`
   to `--parameter-overrides`. The stack then creates only the repository-specific role.

4. Add these GitHub repository variables:

   - `AWS_DEPLOY_ROLE_ARN`
   - `POSTGRES_PARAMETER_NAME`
   - `MISTRAL_PARAMETER_NAME`
   - `BUDGET_ALERT_EMAIL`

5. In Mistral Studio, set the workspace spending limit to USD 10.
6. Run the `Deploy Private Alpha to AWS` workflow manually for the first deployment and confirm the AWS budget subscription email.

## Create tester credentials

Run the administrator CLI from a trusted machine with `POSTGRES_DSN` configured:

```powershell
python -m app.alpha_admin create --username tester.one
python -m app.alpha_admin list
python -m app.alpha_admin disable --username tester.one
python -m app.alpha_admin reset --username tester.one
```

The generated password is printed once. Send it privately and do not store it in GitHub, AWS environment variables, or repository files.

## Verification and cutover

1. Open the `DashboardUrl` CloudFormation output and verify `/health` without credentials.
2. Verify the dashboard rejects missing and invalid credentials.
3. Sign in with a tester account and upload one small repository sample.
4. Confirm `processing_jobs` reaches `STORED` or `REVIEW_REQUIRED`, the ledger record exists, and the source object moved from `inbox/` to `archive/`.
5. Confirm S3 public access is blocked, lifecycle rules exist, both log groups retain seven days, and the annual budget is active.
6. Give testers the Lambda URL, observe it alongside Render, then stop Render. The old R2 files are intentionally not copied or deleted.

The old GitHub R2 polling workflow is manual-only for rollback. Do not re-enable its schedule after S3 cutover.
