{{
  config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
  )
}}

/*
  SCD2 Implementation for type_etat_compteur

  Same logic as communes SCD2 model
*/

-- Step 1: Collect source table
with all_etat_compteur as (
  select
    etat_compteur_id,
    etat_compteur,
    'REF' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('public', 'type_etat_compteur') }}
),

-- Step 2: Prepare source data with natural key (src_id)
source_data as (
  select
    src || '_' || etat_compteur_id as src_id,
    etat_compteur_id,
    etat_compteur,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    row_number() over (
      partition by src || '_' || etat_compteur_id
      order by _ab_cdc_updated_at desc
    ) as rn
  from all_etat_compteur
  {% if is_incremental() %}
  where _ab_cdc_updated_at >
        (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
),

-- Step 3: Dedup
source_data_deduped as (
  select
    src_id,
    etat_compteur_id,
    etat_compteur,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at
  from source_data
  where rn = 1
)

{% if is_incremental() %}

-- Step 4: Identify changed records
, changed_records as (
  select
    s.src_id,
    s.etat_compteur_id,
    s.etat_compteur,
    s.src,
    s._ab_cdc_updated_at,
    s._ab_cdc_deleted_at
  from source_data_deduped s
  inner join {{ this }} t
    on s.src_id = t.src_id
   and t.is_current = true
  where
    s.etat_compteur is distinct from t.etat_compteur
)

-- Step 5: Expire old versions
, expired_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.etat_compteur_id,
    t.etat_compteur,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    c._ab_cdc_updated_at as valid_to,
    false as is_current
  from {{ this }} t
  inner join changed_records c
    on t.src_id = c.src_id
  where t.is_current = true
)

-- Step 6: New records
, new_records as (
  select
    s.*
  from source_data_deduped s
  left join {{ this }} t
    on s.src_id = t.src_id
  where t.src_id is null
    and s._ab_cdc_deleted_at is null
)

-- Step 7: Deleted records
, deleted_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.etat_compteur_id,
    t.etat_compteur,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    s._ab_cdc_deleted_at as valid_to,
    false as is_current
  from {{ this }} t
  inner join all_etat_compteur s
    on t.src_id = (s.src || '_' || s.etat_compteur_id)
  where t.is_current = true
    and s._ab_cdc_deleted_at is not null
)

-- Step 8: Records to insert
, records_to_insert as (
  select * from new_records
  union all
  select
    src_id,
    etat_compteur_id,
    etat_compteur,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at
  from changed_records
)

-- Step 9: Final inserts
, final_inserts as (
  select
    {{ dbt_utils.surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key,
    src_id,
    etat_compteur_id,
    etat_compteur,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_updated_at as valid_from,
    null::timestamp as valid_to,
    true as is_current
  from records_to_insert
)

-- Step 10
select * from expired_records
union all
select * from deleted_records
union all
select * from final_inserts

{% else %}

-- Initial Load
select
  {{ dbt_utils.surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key,
  src_id,
  etat_compteur_id,
  etat_compteur,
  src,
  _ab_cdc_updated_at,
  _ab_cdc_updated_at as valid_from,
  null::timestamp as valid_to,
  true as is_current
from source_data_deduped

{% endif %}
