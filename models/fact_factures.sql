{{
  config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
  )
}}

/*
  SCD2 Implementation for Factures
  
  This model tracks historical changes to facture records from multiple sources.
  It maintains a complete history by creating new rows for each change, with validity dates
  and is_current flag to identify active records.
  
  Key Features:
  - Combines data from AHS,CSM,CZ,OCC,SAHARA factures sources
  - Tracks changes to all business attributes
  - Maintains validity periods (valid_from, valid_to)
  - Identifies current records with is_current flag
  - Uses surrogate keys for unique version identification
  - References dim_date for date_fact using date_id
  
  SCD2 Columns:
  - surrogate_key: Unique identifier for each record version
  - valid_from: Timestamp when this version became active
  - valid_to: Timestamp when this version expired (NULL for current records)
  - is_current: Boolean flag (TRUE for current, FALSE for historical)
*/


-- Step 1: Collect all factures from all RAW source tables
with all_factures as (
  select
    numero_facture as num_facture,
    id as facture_id,
    exercice_id,
    p_eau_id,
    unite_id as usagers_id,
    date_facture as date_fact,
    montant_sold as montant_anterieur,
    montant_net as montant_paye,
    montant_brute as montant_total,
    volume,
    'AHS' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'ahs_factures') }}
  
  union all
  
  select
    numero_facture,
    id,
    exercice_id,
    p_eau_id,
    unite_id,
    date_facture,
    montant_sold,
    montant_net,
    montant_brute,
    volume,
    'CSM' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'csm_factures') }}
  
  union all
  
  select
    numero_facture,
    id,
    exercice_id,
    p_eau_id,
    unite_id,
    date_facture,
    montant_sold,
    montant_net,
    montant_brute,
    volume,
    'OCC' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'occ_factures') }}
  
  union all
  
  select
    numero_facture,
    id,
    exercice_id,
    p_eau_id,
    unite_id,
    date_facture,
    montant_sold,
    montant_net,
    montant_brute,
    volume,
    'CZ' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'cz_factures') }}
  
  union all
  
  select
    numero_facture,
    id,
    exercice_id,
    p_eau_id,
    unite_id,
    date_facture,
    montant_sold,
    montant_net,
    montant_brute,
    volume,
    'SAHARA' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'sahara_factures') }}
),

-- Step 2: Prepare source data with natural key (src_id) and apply incremental filter
source_data as (
  select
    src || '_' || facture_id as src_id,
    num_facture,
    facture_id,
    exercice_id,
    p_eau_id,
    usagers_id,
    date_fact,
    montant_anterieur,
    montant_paye,
    montant_total,
    volume,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    row_number() over (partition by src || '_' || facture_id order by _ab_cdc_updated_at desc) as rn
  from all_factures
  {% if is_incremental() %}
  where _ab_cdc_updated_at > (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
),

-- Dedup (in case a row is added then modified before syncing)
source_data_deduped as (
  select
    src_id,
    num_facture,
    facture_id,
    exercice_id,
    p_eau_id,
    usagers_id,
    date_fact,
    montant_anterieur,
    montant_paye,
    montant_total,
    volume,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at
  from source_data
  where rn = 1
),

-- Step 3: Join with dim_date to get date_id
source_with_date_id as (
  select
    s.src_id,
    s.num_facture,
    s.facture_id,
    s.exercice_id,
    s.p_eau_id,
    s.usagers_id,
    d.date_id as date_fact_id,  -- Get date_id from dimension
    s.montant_anterieur,
    s.montant_paye,
    s.montant_total,
    s.volume,
    s.src,
    s._ab_cdc_updated_at,
    s._ab_cdc_deleted_at
  from source_data_deduped s
  left join {{ ref('dim_date') }} d
    on s.date_fact::date = d.full_date
)

{% if is_incremental() %}
-- Step 4: Identify records that have changed by comparing source to current target records
, changed_records as (
  select
    s.src_id,
    s.num_facture,
    s.facture_id,
    s.exercice_id,
    s.p_eau_id,
    s.usagers_id,
    s.date_fact_id,
    s.montant_anterieur,
    s.montant_paye,
    s.montant_total,
    s.volume,
    s.src,
    s._ab_cdc_updated_at,
    s._ab_cdc_deleted_at
  from source_with_date_id s
  inner join {{ this }} t
    on s.src_id = t.src_id
    and t.is_current = true
  where
    (s.num_facture is distinct from t.num_facture)
    or (s.exercice_id is distinct from t.exercice_id)
    or (s.p_eau_id is distinct from t.p_eau_id)
    or (s.usagers_id is distinct from t.usagers_id)
    or (s.date_fact_id is distinct from t.date_fact_id)  -- Compare date_id instead of date
    or (s.montant_anterieur is distinct from t.montant_anterieur)
    or (s.montant_paye is distinct from t.montant_paye)
    or (s.montant_total is distinct from t.montant_total)
    or (s.volume is distinct from t.volume)
)

-- Step 5: Expire old versions of changed records
, expired_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.num_facture,
    t.facture_id,
    t.exercice_id,
    t.p_eau_id,
    t.usagers_id,
    t.date_fact_id,
    t.montant_anterieur,
    t.montant_paye,
    t.montant_total,
    t.volume,
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

-- Step 6: Identify completely new records
, new_records as (
  select
    s.*
  from source_with_date_id s
  left join {{ this }} t
    on s.src_id = t.src_id
  where t.src_id is null
    and s._ab_cdc_deleted_at is null
)

-- Step 7: Detect deleted records
, deleted_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.num_facture,
    t.facture_id,
    t.exercice_id,
    t.p_eau_id,
    t.usagers_id,
    t.date_fact_id,
    t.montant_anterieur,
    t.montant_paye,
    t.montant_total,
    t.volume,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    s._ab_cdc_deleted_at as valid_to,
    false as is_current
  from {{ this }} t
  inner join all_factures s
    on t.src_id = (s.src || '_' || s.facture_id)
  where t.is_current = true
    and s._ab_cdc_deleted_at is not null
)

-- Step 8: Combine new records and new versions of changed records
, records_to_insert as (
  select * from new_records

  union all

  select 
    src_id,
    num_facture,
    facture_id,
    exercice_id,
    p_eau_id,
    usagers_id,
    date_fact_id,
    montant_anterieur,
    montant_paye,
    montant_total,
    volume,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at
  from changed_records
)

-- Step 9: Add SCD2 metadata columns to records being inserted
, final_inserts as (
  select
    {{ dbt_utils.surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key,
    src_id,
    num_facture,
    facture_id,
    exercice_id,
    p_eau_id,
    usagers_id,
    date_fact_id,
    montant_anterieur,
    montant_paye,
    montant_total,
    volume,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_updated_at as valid_from,
    null::timestamp as valid_to,
    true as is_current
  from records_to_insert
)

-- Step 10: Return both expired records and new inserts
select * from expired_records
union all
select * from deleted_records
union all
select * from final_inserts

{% else %}

-- Initial Load: All records are current with open-ended validity
select
  {{ dbt_utils.surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key,
  src_id,
  num_facture,
  facture_id,
  exercice_id,
  p_eau_id,
  usagers_id,
  date_fact_id,
  montant_anterieur,
  montant_paye,
  montant_total,
  volume,
  src,
  _ab_cdc_updated_at,
  _ab_cdc_updated_at as valid_from,
  null::timestamp as valid_to,
  true as is_current
from source_with_date_id

{% endif %}