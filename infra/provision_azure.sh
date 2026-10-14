#!/usr/bin/env bash
# Provision the Azure resources for uber-taxi-agentic-etl with the Azure CLI.
#
#   az login
#   export PREFIX=uberetl LOCATION=centralindia
#   bash infra/provision_azure.sh
#
# Creates: resource group, ADLS Gen2 account (HNS) + containers per layer, Key Vault,
# service principal with Storage Blob Data Contributor + Cognitive Services OpenAI User,
# Azure Databricks workspace, Azure AI Foundry (Azure OpenAI) resource + GPT deployment,
# and a Key Vault-backed Databricks secret scope.
# Idempotent where the CLI allows it; review before running in a shared subscription.
set -euo pipefail

PREFIX="${PREFIX:-uberetl}"
LOCATION="${LOCATION:-centralindia}"
RG="${RG:-rg-${PREFIX}}"
STORAGE="${STORAGE:-${PREFIX}adls$RANDOM}"
KV="${KV:-kv-${PREFIX}-$RANDOM}"
DBX="${DBX:-dbw-${PREFIX}}"
AOAI="${AOAI:-aoai-${PREFIX}-$RANDOM}"
MODEL="${MODEL:-gpt-4o}"
MODEL_VERSION="${MODEL_VERSION:-2024-11-20}"
SP_NAME="${SP_NAME:-sp-${PREFIX}-etl}"
SCOPE="${SCOPE:-uber-etl}"

echo ">> Resource group $RG ($LOCATION)"
az group create -n "$RG" -l "$LOCATION" -o none

echo ">> ADLS Gen2 storage account $STORAGE"
az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" --sku Standard_LRS --kind StorageV2 \
  --hns true --min-tls-version TLS1_2 --allow-blob-public-access false -o none
for c in raw bronze silver gold quarantine reports; do
  az storage container create --account-name "$STORAGE" -n "$c" --auth-mode login -o none
done

echo ">> Service principal $SP_NAME"
SP_JSON=$(az ad sp create-for-rbac -n "$SP_NAME" --skip-assignment -o json)
SP_APP_ID=$(echo "$SP_JSON" | python -c "import sys,json;print(json.load(sys.stdin)['appId'])")
SP_SECRET=$(echo "$SP_JSON" | python -c "import sys,json;print(json.load(sys.stdin)['password'])")
TENANT_ID=$(az account show --query tenantId -o tsv)
STORAGE_ID=$(az storage account show -n "$STORAGE" -g "$RG" --query id -o tsv)
az role assignment create --assignee "$SP_APP_ID" --role "Storage Blob Data Contributor" --scope "$STORAGE_ID" -o none

echo ">> Key Vault $KV (secrets for the Databricks secret scope)"
az keyvault create -n "$KV" -g "$RG" -l "$LOCATION" --enable-rbac-authorization false -o none
az keyvault secret set --vault-name "$KV" -n sp-client-id --value "$SP_APP_ID" -o none
az keyvault secret set --vault-name "$KV" -n sp-client-secret --value "$SP_SECRET" -o none
az keyvault secret set --vault-name "$KV" -n sp-tenant-id --value "$TENANT_ID" -o none

echo ">> Azure Databricks workspace $DBX"
az extension add --name databricks --upgrade -y -o none
az databricks workspace create -n "$DBX" -g "$RG" -l "$LOCATION" --sku premium -o none
DBX_URL="https://$(az databricks workspace show -n "$DBX" -g "$RG" --query workspaceUrl -o tsv)"

echo ">> Microsoft Foundry / Azure OpenAI resource $AOAI + deployment $MODEL"
az cognitiveservices account create -n "$AOAI" -g "$RG" -l "$LOCATION" --kind OpenAI --sku S0 \
  --custom-domain "$AOAI" -o none
az cognitiveservices account deployment create -n "$AOAI" -g "$RG" --deployment-name "$MODEL" \
  --model-name "$MODEL" --model-version "$MODEL_VERSION" --model-format OpenAI \
  --sku-name GlobalStandard --sku-capacity 20 -o none
AOAI_ID=$(az cognitiveservices account show -n "$AOAI" -g "$RG" --query id -o tsv)
AOAI_ENDPOINT=$(az cognitiveservices account show -n "$AOAI" -g "$RG" --query properties.endpoint -o tsv)
# Entra ID auth for the agents (no API keys needed)
az role assignment create --assignee "$SP_APP_ID" --role "Cognitive Services OpenAI User" --scope "$AOAI_ID" -o none

cat <<EOF

Done. Next steps
----------------
1. Create a Key Vault-backed secret scope named '$SCOPE' in the Databricks UI:
     $DBX_URL#secrets/createScope
     DNS name:    https://$KV.vault.azure.net/
     Resource ID: $(az keyvault show -n "$KV" --query id -o tsv)
2. Configure the Databricks CLI:  databricks configure --host $DBX_URL
3. Deploy the jobs:
     databricks bundle deploy -t dev --var storage_account=$STORAGE \\
       --var foundry_endpoint=$AOAI_ENDPOINT --var foundry_deployment=$MODEL --var alert_email=<you>
4. Upload the source CSV to the landing zone:
     az storage fs file upload --account-name $STORAGE -f raw --auth-mode login \\
       -s data/ncr_ride_bookings.csv -p uber_rides/ncr_ride_bookings.csv
5. Local .env values:
     STORAGE_ACCOUNT_NAME=$STORAGE
     DATABRICKS_HOST=$DBX_URL
     AZURE_OPENAI_ENDPOINT=$AOAI_ENDPOINT
     AZURE_OPENAI_DEPLOYMENT=$MODEL
EOF
