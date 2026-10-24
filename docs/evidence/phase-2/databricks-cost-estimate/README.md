# Databricks cost estimate — 2026-10-24 KST

Read-only settings/pricing investigation only. No Compute/Job/SQL start, AWS/IAM/UC changes,
Kafka publish, staging/commit/push/merge or repeated local tests.

- observed-config.json: freshly queried cluster, selected node type, Job/library status. Workspace
  user email in the source path is redacted. DBU/h is not a returned API measurement.
- prices-and-costs.json: selected official calculator rows, AWS Price List SKU/effectiveDate,
  decimal cost calculations, conditional Serverless sensitivity and proposed execution budget.
- billing-and-safety.json: account-auth/credit/invoice limits and no-execution status.

Read-only commands executed:

```sh
git status --short
git branch --show-current
git rev-parse HEAD
git diff --check
databricks clusters get 0929-113111-cgu7k91c -p ktd -o json
databricks clusters list-node-types -p ktd -o json
databricks libraries cluster-status 0929-113111-cgu7k91c -p ktd -o json
databricks jobs get 50294370651632 -p ktd -o json
databricks jobs list-runs --active-only -p ktd -o json
databricks current-user me -p ktd -o json
databricks workspace-conf get-status enableServerlessCompute -p ktd -o json
# Workspace config lookup failed: serverless enablement is UNVERIFIED.
databricks account billable-usage download 2026-10 2026-10 -p ktd -o json
# Workspace-host account endpoint returned Not Found. Correct account-host config also lacked auth.
```

AWS Pricing GetProducts filters: AmazonEC2, location=Asia Pacific (Seoul), instanceType=r5d.large,
operatingSystem=Linux, tenancy=Shared, preInstalledSw=NA, capacitystatus=Used, endpoint us-east-1.
Returned one OnDemand price $0.173/hour. No chargeable SQL/Compute was used for pricing/billing.
Official calculator was inspected through its public UI; public UI plan selection does not prove
this account's plan. Premium AP Photon row0.900/$0.4950, Enterprise0.900/$0.5850;
Classic Jobs Premium0.450/$0.0675, Enterprise0.450/$0.0900;
Jobs Photon Premium1.305/$0.1958 (display rounded). Premium Serverless selector$0.40/DBU.

Sources:

- https://www.databricks.com/product/pricing/product-pricing/instance-types
- https://aws.amazon.com/ec2/pricing/on-demand/
- https://docs.databricks.com/aws/en/jobs/compute
- https://docs.databricks.com/aws/en/jobs/run-serverless-jobs
- https://docs.databricks.com/aws/en/compute/serverless/limitations
- https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts
- https://docs.databricks.com/aws/en/compute/standard-limitations
- https://docs.databricks.com/aws/en/spark/api-options
- https://docs.databricks.com/en/user-guide/clusters/sparkui.html

Validation: JSON parse + cost formula assertions + existing Runbook prefix hash + credential-pattern
scan + git diff --check. No old test result is represented as newly executed. The account billing
and Serverless workspace flag failures are control-plane access/lookup limitations, not zero cost.

After user review only (not executed):

```sh
git add -- docs/evidence/phase-2/databricks-cost-estimate/observed-config.json \
  docs/evidence/phase-2/databricks-cost-estimate/prices-and-costs.json \
  docs/evidence/phase-2/databricks-cost-estimate/billing-and-safety.json \
  docs/evidence/phase-2/databricks-cost-estimate/README.md
git add -p -- docs/implementation/p2-8i-cloud-e2e-runbook.md
# Select only the new 2026-10-24 Databricks cost section; split/edit mixed hunks.
git diff --cached --check
git diff --cached
git commit -m "docs(phase2): estimate Databricks E2E and recovery execution costs"
```

The report file and all other preexisting modified/untracked work remain outside this suggested
commit. main merge remains blocked until full Phase2 DoD.
