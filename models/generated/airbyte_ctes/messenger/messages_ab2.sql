{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_messenger",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('messages_ab1') }}
select
    cast(id as {{ dbt_utils.type_string() }}) as id,
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(type as {{ dbt_utils.type_float() }}) as type,
    cast(bot_id as {{ dbt_utils.type_string() }}) as bot_id,
    {{ cast_to_boolean('is_bot') }} as is_bot,
    cast(opened as {{ dbt_utils.type_string() }}) as opened,
    cast(sent_at as {{ dbt_utils.type_string() }}) as sent_at,
    cast(user_id as {{ dbt_utils.type_float() }}) as user_id,
    cast(direction as {{ dbt_utils.type_float() }}) as direction,
    cast(messenger as {{ dbt_utils.type_string() }}) as messenger,
    cast(contact_id as {{ dbt_utils.type_string() }}) as contact_id,
    cast(campaign_id as {{ dbt_utils.type_string() }}) as campaign_id,
    cast(delivered_at as {{ dbt_utils.type_string() }}) as delivered_at,
    cast(campaign_contact as {{ dbt_utils.type_string() }}) as campaign_contact,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('messages_ab1') }}
-- messages
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

