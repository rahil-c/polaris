#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Prerequisites:
#   - CLIENT_ID and CLIENT_SECRET must already be exported (admin or service account).
#   - jq must be installed for JSON parsing.
#   - DEFAULT_BASE_LOCATION is optional (defaults to /tmp/quickstart_catalog).
# ------------------------------------------------------------------------------

# Check for required env vars
if [[ -z "${CLIENT_ID:-}" || -z "${CLIENT_SECRET:-}" ]]; then
  echo "ERROR: Please export CLIENT_ID and CLIENT_SECRET (your admin Polaris credentials)."
  exit 1
fi

# Check that 'jq' is available
if ! command -v jq &>/dev/null; then
  echo "ERROR: 'jq' is required but not found. Install jq and retry."
  exit 1
fi

# ------------------------------------------------------------------------------
# 1) Create a new catalog named 'quickstart_catalog'
# ------------------------------------------------------------------------------
echo "=== Creating catalog 'quickstart_catalog' (storage-type=s3) ==="
./polaris \
  --client-id "${CLIENT_ID}" \
  --client-secret "${CLIENT_SECRET}" \
  catalogs \
  create \
  --storage-type s3 \
  --default-base-location "${DEFAULT_BASE_LOCATION}" \
  --role-arn "${ROLE_ARN}" \
  quickstart_catalog

# ------------------------------------------------------------------------------
# 2) Create a new principal named 'quickstart_user'
# ------------------------------------------------------------------------------
echo
echo "=== Creating principal 'quickstart_user' ==="
CREDS_JSON="$(
  ./polaris \
    --client-id "${CLIENT_ID}" \
    --client-secret "${CLIENT_SECRET}" \
    principals create \
    quickstart_user
)"
echo "Output from principals create:"
echo "${CREDS_JSON}"
echo

# ------------------------------------------------------------------------------
# 3) Extract the real clientId/clientSecret and export them (and persist to ~/.zshrc)
# ------------------------------------------------------------------------------
export USER_CLIENT_ID="$(echo "${CREDS_JSON}" | jq -r '.clientId')"
export USER_CLIENT_SECRET="$(echo "${CREDS_JSON}" | jq -r '.clientSecret')"

if [[ -z "${USER_CLIENT_ID}" || -z "${USER_CLIENT_SECRET}" ]]; then
  echo "ERROR: Could not parse clientId/clientSecret from JSON."
  exit 1
fi

echo "Exported USER_CLIENT_ID=${USER_CLIENT_ID}"
echo "Exported USER_CLIENT_SECRET=${USER_CLIENT_SECRET}"
echo

RC_FILE="$HOME/.zshrc"

# Remove any existing lines that export USER_CLIENT_ID or USER_CLIENT_SECRET,
# then append the new values once.
if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' '/export USER_CLIENT_ID=/d' "$RC_FILE"
  sed -i '' '/export USER_CLIENT_SECRET=/d' "$RC_FILE"
else
  sed -i '/export USER_CLIENT_ID=/d' "$RC_FILE"
  sed -i '/export USER_CLIENT_SECRET=/d' "$RC_FILE"
fi

echo "export USER_CLIENT_ID=${USER_CLIENT_ID}" >> "$RC_FILE"
echo "export USER_CLIENT_SECRET=${USER_CLIENT_SECRET}" >> "$RC_FILE"

echo "Set USER_CLIENT_ID and USER_CLIENT_SECRET in $RC_FILE"
echo "To load them into your current shell, run: source ~/.zshrc"


# ------------------------------------------------------------------------------
# 4) Create a principal-role named 'quickstart_user_role'
# ------------------------------------------------------------------------------
echo "=== Creating principal-role 'quickstart_user_role' ==="
./polaris \
  --client-id "${CLIENT_ID}" \
  --client-secret "${CLIENT_SECRET}" \
  principal-roles create \
  quickstart_user_role

# ------------------------------------------------------------------------------
# 5) Create a catalog-role named 'quickstart_catalog_role' bound to 'quickstart_catalog'
# ------------------------------------------------------------------------------
echo
echo "=== Creating catalog-role 'quickstart_catalog_role' for catalog 'quickstart_catalog' ==="
./polaris \
  --client-id "${CLIENT_ID}" \
  --client-secret "${CLIENT_SECRET}" \
  catalog-roles create \
    --catalog quickstart_catalog \
    quickstart_catalog_role

# ------------------------------------------------------------------------------
# 6) Grant the principal-role to the principal
# ------------------------------------------------------------------------------
echo
echo "=== Granting principal-role 'quickstart_user_role' to principal 'quickstart_user' ==="
./polaris \
  --client-id "${CLIENT_ID}" \
  --client-secret "${CLIENT_SECRET}" \
  principal-roles grant \
    --principal quickstart_user \
    quickstart_user_role

# ------------------------------------------------------------------------------
# 7) Grant the catalog-role to the principal-role
# ------------------------------------------------------------------------------
echo
echo "=== Granting catalog-role 'quickstart_catalog_role' (on catalog 'quickstart_catalog') to principal-role 'quickstart_user_role' ==="
./polaris \
  --client-id "${CLIENT_ID}" \
  --client-secret "${CLIENT_SECRET}" \
  catalog-roles grant \
    --catalog quickstart_catalog \
    --principal-role quickstart_user_role \
    quickstart_catalog_role

# ------------------------------------------------------------------------------
# 8) Grant CATALOG_MANAGE_CONTENT privilege to that catalog-role
# ------------------------------------------------------------------------------
echo
echo "=== Granting 'CATALOG_MANAGE_CONTENT' to catalog-role 'quickstart_catalog_role' on 'quickstart_catalog' ==="
./polaris \
  --client-id "${CLIENT_ID}" \
  --client-secret "${CLIENT_SECRET}" \
  privileges catalog grant \
    --catalog quickstart_catalog \
    --catalog-role quickstart_catalog_role \
    CATALOG_MANAGE_CONTENT
