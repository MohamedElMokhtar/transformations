{{ config(materialized='incremental', unique_key='id') }}

select
    *,
    'AHS' as ABH
from {{ source('public', 'ahs_factures') }}

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}
