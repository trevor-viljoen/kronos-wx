#!/bin/bash
# scripts/redeploy.sh
# Push → wait for CI → redeploy kronos-wx stack on Portainer 173

set -euo pipefail

CRED_FILE="$HOME/config/.portainer"
if [ ! -f "$CRED_FILE" ]; then
    echo "Error: credentials not found at $CRED_FILE"
    exit 1
fi
source "$CRED_FILE"

URL="$PORTAINER_173_URL"
API_KEY="$PORTAINER_173_API_KEY"
ENDPOINT_ID="$PORTAINER_173_ENDPOINT_ID"
STACK_NAME="kronos-wx"

BRANCH=$(git rev-parse --abbrev-ref HEAD)
SHORT_HASH=$(git rev-parse --short HEAD)

echo "🚀 Redeploying $STACK_NAME from $BRANCH ($SHORT_HASH)"

if ! command -v gh &>/dev/null; then
    echo "Error: 'gh' CLI not found"
    exit 1
fi

# Wait for the Docker build workflow specifically (not CodeQL or other workflows)
echo "⌛ Waiting for Docker build CI run to appear..."
RUN_ID=""
for i in $(seq 1 20); do
    RUN_ID=$(gh run list --repo trevor-viljoen/kronos-wx --branch "$BRANCH" \
        --workflow "docker.yml" --limit 1 \
        --json databaseId,headSha --jq \
        ".[] | select(.headSha == \"$(git rev-parse HEAD)\") | .databaseId" \
        2>/dev/null || true)
    [ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ] && break
    sleep 5
done

if [ -z "$RUN_ID" ] || [ "$RUN_ID" = "null" ]; then
    echo "❌ No Docker build CI run found for $SHORT_HASH on branch $BRANCH"
    exit 1
fi

echo "👁️  Watching Docker build run $RUN_ID..."
gh run watch "$RUN_ID" --repo trevor-viljoen/kronos-wx --exit-status || {
    echo "❌ Docker build failed — skipping Portainer redeploy"
    exit 1
}

# Force-pull both images so Portainer always gets the freshly built layers
echo "📦 Pulling updated images..."
for IMAGE in kronos-wx-backend kronos-wx-frontend; do
    curl -k -s -o /dev/null -X POST \
        "$URL/api/endpoints/$ENDPOINT_ID/docker/images/create?fromImage=ghcr.io%2Ftrevor-viljoen%2F${IMAGE}&tag=latest" \
        -H "X-API-Key: $API_KEY"
    echo "  ✓ ghcr.io/trevor-viljoen/${IMAGE}:latest"
done

# Resolve stack ID by name
STACK_ID=$(curl -k -s -H "X-API-Key: $API_KEY" "$URL/api/stacks" \
    | python3 -c "import sys,json; s=next((s for s in json.load(sys.stdin) if s['Name']=='$STACK_NAME'),None); print(s['Id'] if s else '')" 2>/dev/null)

if [ -z "$STACK_ID" ]; then
    echo "❌ Stack '$STACK_NAME' not found on Portainer — create it first"
    exit 1
fi

echo "🔄 Triggering Portainer redeploy (stack $STACK_ID)..."
HTTP=$(curl -k -s -o /dev/null -w "%{http_code}" -X PUT \
    "$URL/api/stacks/$STACK_ID/git/redeploy?endpointId=$ENDPOINT_ID" \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{
      \"pullImage\": true,
      \"env\": [
        {\"name\": \"NWS_CONTACT_EMAIL\",       \"value\": \"${KRONOS_NWS_CONTACT_EMAIL:-kronos-wx-operator}\"},
        {\"name\": \"VAPID_CONTACT\",            \"value\": \"${KRONOS_VAPID_CONTACT:-mailto:kronos@localhost}\"},
        {\"name\": \"LOG_LEVEL\",               \"value\": \"${KRONOS_LOG_LEVEL:-INFO}\"},
        {\"name\": \"WAR_ROOM_ENABLED\",        \"value\": \"${KRONOS_WAR_ROOM_ENABLED:-false}\"},
        {\"name\": \"WAR_ROOM_TABLO_WEB_HOST\", \"value\": \"${KRONOS_WAR_ROOM_TABLO_WEB_HOST:-}\"},
        {\"name\": \"WAR_ROOM_KOCO_ID\",        \"value\": \"${KRONOS_WAR_ROOM_KOCO_ID:-}\"},
        {\"name\": \"WAR_ROOM_KFOR_ID\",        \"value\": \"${KRONOS_WAR_ROOM_KFOR_ID:-}\"},
        {\"name\": \"WAR_ROOM_KWTV_ID\",        \"value\": \"${KRONOS_WAR_ROOM_KWTV_ID:-}\"}
      ]
    }")

if [ "$HTTP" = "200" ] || [ "$HTTP" = "204" ]; then
    echo "✅ Redeploy triggered (HTTP $HTTP)"
else
    echo "❌ Redeploy failed (HTTP $HTTP)"
    exit 1
fi
