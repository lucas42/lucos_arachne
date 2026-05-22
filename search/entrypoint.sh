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

	PAYLOAD_JSON=$(jq -n --arg key "$KEY" --arg system_env "$SYSTEM_ENV" --argjson actions "$ACTIONS" '{value: $key, actions: $actions, collections: ["*"], description: $system_env}')
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
		KEY_ID=$(echo "$EXISTING_KEYS_JSON" | jq -r ".keys[] | select(.description==\"$EXISTING_SYSTEM\") | .id")
		echo "Revoking stale key ($EXISTING_SYSTEM)"
		curl -X DELETE "${TYPESENSE_URL}/keys/${KEY_ID}" \
			-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" --silent --show-error --fail > /dev/null
	fi
done

# Check if collection already exists
ITEMS_COLLECTION_JSON=$(curl -s -H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
	"${TYPESENSE_URL}/collections/items")
if echo "$ITEMS_COLLECTION_JSON" | grep -q '"name":"items"'; then
	echo "Collection 'items' already exists. Checking for missing optional fields..."
	EXISTING_FIELDS=$(echo "$ITEMS_COLLECTION_JSON" | jq -r '.fields[].name' 2>/dev/null)
	for FIELD_NAME in contained_in artist; do
		if echo "$EXISTING_FIELDS" | grep -q "^${FIELD_NAME}$"; then
			echo "  Field '${FIELD_NAME}' already present."
		else
			echo "  Adding missing field '${FIELD_NAME}'..."
			curl -X PATCH "${TYPESENSE_URL}/collections/items" \
				-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
				-H "Content-Type: application/json" \
				-d "{\"fields\": [{\"name\": \"${FIELD_NAME}\", \"type\": \"string\", \"optional\": true}]}" \
				--silent --show-error --fail-with-body
			echo "  Field '${FIELD_NAME}' added."
		fi
	done
	# Person-merge fields (secondary_uris for lazy lookup, is_contact for filtering)
	if echo "$EXISTING_FIELDS" | grep -q "^secondary_uris$"; then
		echo "  Field 'secondary_uris' already present."
	else
		echo "  Adding missing field 'secondary_uris'..."
		curl -X PATCH "${TYPESENSE_URL}/collections/items" \
			-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
			-H "Content-Type: application/json" \
			-d '{"fields": [{"name": "secondary_uris", "type": "string[]", "optional": true}]}' \
			--silent --show-error --fail-with-body
		echo "  Field 'secondary_uris' added."
	fi
	if echo "$EXISTING_FIELDS" | grep -q "^is_contact$"; then
		echo "  Field 'is_contact' already present."
	else
		echo "  Adding missing field 'is_contact'..."
		curl -X PATCH "${TYPESENSE_URL}/collections/items" \
			-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
			-H "Content-Type: application/json" \
			-d '{"fields": [{"name": "is_contact", "type": "bool", "optional": true}]}' \
			--silent --show-error --fail-with-body
		echo "  Field 'is_contact' added."
	fi
else
	echo "Creating 'items' collection..."
	curl -X POST "${TYPESENSE_URL}/collections" \
		-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
		-H "Content-Type: application/json" \
		-d '{
			"name": "items",
			"fields": [
				{"name": "type", "type": "string", "facet": true},
				{"name": "category", "type": "string", "facet": true},
				{"name": "pref_label", "type": "string", "full_text_search": true, "sort": true},
				{"name": "labels", "type": "string[]", "optional": true, "full_text_search": true},
				{"name": "description", "type": "string", "optional": true, "full_text_search": true},
				{"name": "lyrics", "type": "string", "optional": true, "full_text_search": true},
				{"name": "lang_family", "type": "string", "optional": true},
				{"name": "contained_in", "type": "string", "optional": true},
				{"name": "artist", "type": "string", "optional": true},
				{"name": "secondary_uris", "type": "string[]", "optional": true},
				{"name": "is_contact", "type": "bool", "optional": true}
			],
			"default_sorting_field":"pref_label"
		}' --silent --show-error --fail-with-body
	echo -e "\nCollection 'items' created."
fi

# Check if tracks collection already exists
if curl -s -H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
	"${TYPESENSE_URL}/collections/tracks" | grep -q '"name":"tracks"'; then
	echo "Collection 'tracks' already exists, skipping schema creation."
else
	echo "Creating 'tracks' collection..."
	curl -X POST "${TYPESENSE_URL}/collections" \
		-H "X-TYPESENSE-API-KEY: ${TYPESENSE_ADMIN_KEY}" \
		-H "Content-Type: application/json" \
		-d '{
			"name": "tracks",
			"fields": [
				{"name": "title", "type": "string", "sort": true},
				{"name": "artist", "type": "string[]", "facet": true, "optional": true},
				{"name": "album", "type": "string[]", "facet": true, "optional": true},
				{"name": "genre", "type": "string[]", "facet": true, "optional": true},
				{"name": "composer", "type": "string[]", "facet": true, "optional": true},
				{"name": "producer", "type": "string[]", "facet": true, "optional": true},
				{"name": "language", "type": "string[]", "facet": true, "optional": true},
				{"name": "year", "type": "string", "facet": true, "optional": true},
				{"name": "rating", "type": "int32", "facet": true, "optional": true},
				{"name": "lyrics", "type": "string", "optional": true, "full_text_search": true},
				{"name": "provenance", "type": "string", "facet": true, "optional": true},
				{"name": "duration", "type": "int32", "optional": true},
				{"name": "offence", "type": "string[]", "facet": true, "optional": true},
				{"name": "comment", "type": "string", "optional": true, "full_text_search": true},
				{"name": "soundtrack", "type": "string[]", "facet": true, "optional": true}
			],
			"default_sorting_field": "title"
		}' --silent --show-error --fail-with-body
	echo -e "\nCollection 'tracks' created."
fi


wait $TYPESENSE_PID
