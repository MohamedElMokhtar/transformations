{{
    config(
        materialized='incremental',
        unique_key=['id', 'ABH']
    )
}}

select
    *,
    'AHS' as ABH
from {{ source('public', 'ahs_factures') }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

union all

select
    *,
    'CSM' as ABH
from {{ source('public', 'csm_factures') }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}