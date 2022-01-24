{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_messenger",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('messenger', '_airbyte_raw_messages') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as type,
    null AS price,
    {{ json_extract_scalar('_airbyte_data', ['bot_id'], ['bot_id']) }} as bot_id,
    json_extract_scalar(_airbyte_data, "$['messenger']['status']")  as status,
    null AS is_paid,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as user_id,
    {{ json_extract_scalar('_airbyte_data', ['campaign_id'], ['campaign_id']) }} as campaign_id,
    null AS currency,
    {{ json_extract_scalar('_airbyte_data', ['direction'], ['direction']) }} as direction,
    {{ json_extract_scalar('_airbyte_data', ['contact_id'], ['contact_id']) }} as contact_id,
    DATETIME_ADD("1970-01-01T00:00:00", INTERVAL IFNULL(SAFE_CAST(CONCAT('0x', REPLACE(SUBSTR(json_extract_scalar(_airbyte_data, "$['_id']"), 1, 8), ' ', '')) AS INT64), 0) SECOND) AS created_at,
    DATETIME_ADD("1970-01-01T00:00:00", INTERVAL IFNULL(SAFE_CAST(CONCAT('0x', REPLACE(SUBSTR(json_extract_scalar(_airbyte_data, "$['_id']"), 1, 8), ' ', '')) AS INT64), 0) SECOND) AS updated_at,
    null AS price_country_code,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['is_bot'], ['is_bot']) }} as is_bot,
    json_extract_scalar(_airbyte_data, "$['opened']['opened_at']")  as opened_at,
    {{ json_extract_scalar('_airbyte_data', ['sent_at'], ['sent_at']) }} as sent_at,
    {{ json_extract_scalar('_airbyte_data', ['delivered_at'], ['delivered_at']) }} as delivered_at,
    json_extract_scalar(_airbyte_data, "$['campaign_contact']['id']") as campaign_contact_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('messenger', '_airbyte_raw_messages') }} as table_alias
-- messages
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

