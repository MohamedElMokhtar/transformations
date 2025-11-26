{{ config(materialized='incremental', unique_key='id') }}

select
    *,
    'CSM' as ABH
from {{ source('public', 'csm_factures') }}

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
