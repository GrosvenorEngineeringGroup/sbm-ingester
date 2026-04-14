#!/usr/bin/env bash
# Compare the Lambda ARNs granted by the sbm-ingester-cicd-policy IAM policy
# against the canonical list declared by Terraform (`cicd_managed_lambda_arns`
# output). Exit 0 if in sync, 1 if drift detected.
#
# Why this exists: the IAM policy is managed manually (not via Terraform) to
# keep access keys out of state and avoid AWS managed-policy version churn.
# Any Lambda rename/add/remove in Terraform must be mirrored into the policy
# by hand. This script catches the case where someone forgot.
#
# Usage:
#   ./scripts/check_cicd_policy_drift.sh
#
# Exit codes:
#   0 — in sync
#   1 — drift (missing / extra ARNs) — human must patch policy
#   2 — prerequisite / tooling error

set -euo pipefail

POLICY_ARN="arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy"
TF_DIR="$(cd "$(dirname "$0")/.." && pwd)/terraform"

command -v jq >/dev/null || { echo "ERROR: jq required" >&2; exit 2; }
command -v terraform >/dev/null || { echo "ERROR: terraform required" >&2; exit 2; }
command -v aws >/dev/null || { echo "ERROR: aws cli required" >&2; exit 2; }

echo "→ Reading canonical list from terraform output..."
expected=$(cd "$TF_DIR" && terraform output -json cicd_managed_lambda_arns | jq -r '.[]' | sort)

echo "→ Fetching live policy from AWS..."
default_version=$(aws iam get-policy --policy-arn "$POLICY_ARN" --query 'Policy.DefaultVersionId' --output text)
actual=$(aws iam get-policy-version --policy-arn "$POLICY_ARN" --version-id "$default_version" \
    --query 'PolicyVersion.Document.Statement[?Sid==`LambdaUpdateFunctions`].Resource[]' --output json \
    | jq -r '.[]' | sort)

missing=$(comm -23 <(echo "$expected") <(echo "$actual"))
extra=$(comm -13 <(echo "$expected") <(echo "$actual"))

if [[ -z "$missing" && -z "$extra" ]]; then
    echo "✓ In sync. Policy grants exactly the $(echo "$expected" | wc -l | tr -d ' ') Lambdas declared in Terraform."
    exit 0
fi

echo "✗ DRIFT DETECTED between Terraform and IAM policy:" >&2
if [[ -n "$missing" ]]; then
    echo ""
    echo "  Missing from policy (Terraform has these, policy doesn't):" >&2
    echo "$missing" | sed 's/^/    /' >&2
fi
if [[ -n "$extra" ]]; then
    echo ""
    echo "  Extra in policy (policy has these, Terraform doesn't):" >&2
    echo "$extra" | sed 's/^/    /' >&2
fi
echo ""
echo "Remediation: manually update the policy JSON and call" >&2
echo "  aws iam create-policy-version --policy-arn $POLICY_ARN \\" >&2
echo "    --policy-document file://<new-policy.json> --set-as-default" >&2
echo "(Delete oldest non-default version first if at 5-version cap.)" >&2
exit 1
