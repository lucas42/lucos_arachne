#!/bin/bash
set -e

: "${PORT:?Can\'t find environment variable PORT}"
: "${TYPESENSE_ADMIN_KEY:?Can\'t find environment variable TYPESENSE_ADMIN_KEY}"



TYPESENSE_URL="http://localhost:${PORT}"

# Start Typesense in background
GLOG_minloglevel=1 /opt/typesense-server --data-dir /data \
	--api-key="$TYPESENSE_ADMIN_KEY" \
	--listen-port="$PORT" \
	--enable-cors &
TYPESENSE_PID=$!

# Wait for the API to be ready
echo "Waiting for Typesense API..."
until curl "${TYPESENSE_URL}/health" --silent --fail > /dev/null; do
	sleep 1
done

declare -A DESIRED_KEYS
if [ -n "$CLIENT_KEYS" ]; then
	for line in $(echo "$CLIENT_KEYS" | tr ';' '\n'); do
		SYSTEM_ENV=$(echo "$line" | cut -d= -f1)
		KEY=$(echo "$line" | cut -d= -f2)

		if [ -z "$SYSTEM_ENV" ] || [ -z "$KEY" ]; then
			continue
		fi

		DESIRED_KEYS["$SYSTEM_ENV"]="$KEY"
	done
fi

# Fetch existing keys
EXISTING_KEYS_JSON=$(curl -s "${TYPESENSE_URL}/keys" \
	-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}")
EXISTING_SYSTEMS=$(echo "$EXISTING_KEYS_JSON" | jq -r '.keys[]?.description')

# Reconcile keys
for SYSTEM_ENV in "${!DESIRED_KEYS[@]}"; do
	KEY=${DESIRED_KEYS[$SYSTEM_ENV]}

	# Extract system name before colon
	SYSTEM_NAME=$(echo "$SYSTEM_ENV" | cut -d: -f1)

	# Default permissions = read
	ACTIONS='["documents:search"]'
	if [ "$SYSTEM_NAME" = "lucos_arachne" ]; then
		ACTIONS='["*"]'
	fi

	PAYLOAD_JSON=$(jq -n --arg key "$KEY" --arg system_env "$SYSTEM_ENV" --argjson actions $ACTIONS '{value: $key, actions: $actions, collections: ["*"], description: $system_env}')
	if echo "$EXISTING_SYSTEMS" | grep -q "^${SYSTEM_ENV}$"; then
		CURRENT_ACTIONS=$(echo "$EXISTING_KEYS_JSON" | jq -c ".keys[] | select(.description==\"$SYSTEM_ENV\") | .actions")
		CURRENT_KEY_PREFIX=$(echo "$EXISTING_KEYS_JSON" | jq -c ".keys[] | select(.description==\"$SYSTEM_ENV\") | .value_prefix" -r )
		if [ "$CURRENT_ACTIONS" != "$ACTIONS" ] || [[ "$KEY" != "$CURRENT_KEY_PREFIX"*  ]]; then
			echo "Updating key for $SYSTEM_ENV with actions $ACTIONS"
			KEY_ID=$(echo "$EXISTING_KEYS_JSON" | jq -r ".keys[] | select(.description==\"$SYSTEM_ENV\") | .id")
			curl -X DELETE "${TYPESENSE_URL}/keys/${KEY_ID}" \
				-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" --silent --show-error --fail > /dev/null
			curl "${TYPESENSE_URL}/keys" \
				-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
				-H "Content-Type: application/json" \
				-d "$PAYLOAD_JSON" --silent --show-error --fail > /dev/null
		else
			echo "Key for $SYSTEM_ENV already present with correct actions"
		fi
	else
		echo "Creating key for $SYSTEM_ENV with actions $ACTIONS"
		curl "${TYPESENSE_URL}/keys" \
			-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
			-H "Content-Type: application/json" \
			-d "$PAYLOAD_JSON" --silent --show-error --fail > /dev/null
	fi
done

# Delete stale keys
for EXISTING_SYSTEM in $EXISTING_SYSTEMS; do
	if [ -z "${DESIRED_KEYS[$EXISTING_SYSTEM]}" ]; then
		KEY_ID=$(echo "$EXISTING_SYSTEMS_JSON" | jq -r ".keys[] | select(.description==\"$EXISTING_SYSTEM\") | .id")
		echo "Revoking stale key ($EXISTING_SYSTEM)"
		curl -X DELETE "${TYPESENSE_URL}/keys/${KEY_ID}" \
			-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_SYSTEM}" --silent --show-error --fail > /dev/null
	fi
done

# Check if collection already exists
if curl -s -H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
	"${TYPESENSE_URL}/collections/items" | grep -q '"name":"items"'; then
	echo "Collection 'items' already exists, skipping schema creation and overrides."
else
	echo "Creating 'items' collection..."
	curl -X POST "${TYPESENSE_URL}/collections" \
		-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
		-H "Content-Type: application/json" \
		-d '{
			"name": "items",
			"fields": [
				{"name": "type", "type": "string", "facet": true},
				{"name": "pref_label", "type": "string", "full_text_search": true, "sort": true},
				{"name": "labels", "type": "string[]", "optional": true, "full_text_search": true},
				{"name": "description", "type": "string", "optional": true, "full_text_search": true},
				{"name": "lyrics", "type": "string", "optional": true, "full_text_search": true},
				{"name": "lang_family", "type": "string", "optional": true}
			],
			"default_sorting_field":"pref_label"
		}' --silent --show-error --fail-with-body
	echo -e "\nCollection 'items' created."
fi


wait $TYPESENSE_PID
