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
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at, -- CDC timestamp for tracking changes
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at  -- CDC timestamp for deletions
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
    row_number() over (partition by src || '_' || facture_id order by _ab_cdc_updated_at desc) as rn  -- ADDED
  from all_factures
  {% if is_incremental() %}
  -- Only process records that have been updated since last run
  where _ab_cdc_updated_at > (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
),

-- Dedup (in case a row is added then modified before syncing)
source_data_deduped as (  -- NEW CTE
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
  where rn = 1  -- Keep only the most recent version
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
    s.date_fact,
    s.montant_anterieur,
    s.montant_paye,
    s.montant_total,
    s.volume,
    s.src,
    s._ab_cdc_updated_at,
    s._ab_cdc_deleted_at
  from source_data_deduped s
  inner join {{ this }} t
    on s.src_id = t.src_id
    and t.is_current = true -- Only compare against current versions
  where
    -- Check if any business attributes have changed
    -- IS DISTINCT FROM handles NULL comparisons correctly
    (s.num_facture is distinct from t.num_facture)
    or (s.exercice_id is distinct from t.exercice_id)
    or (s.p_eau_id is distinct from t.p_eau_id)
    or (s.usagers_id is distinct from t.usagers_id)
    or (s.montant_anterieur is distinct from t.montant_anterieur)
    or (s.montant_paye is distinct from t.montant_paye)
    or (s.montant_total is distinct from t.montant_total)
    or (s.volume is distinct from t.volume)
)

-- Step 5: Expire old versions of changed records by setting end date and is_current flag
, expired_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.num_facture,
    t.facture_id,
    t.exercice_id,
    t.p_eau_id,
    t.usagers_id,
    t.date_fact,
    t.montant_anterieur,
    t.montant_paye,
    t.montant_total,
    t.volume,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,                      -- Keep original start date
    c._ab_cdc_updated_at as valid_to,  -- End when the new version became valid
    false as is_current                -- Mark as historical
  from {{ this }} t
  inner join changed_records c
    on t.src_id = c.src_id
  where t.is_current = true
)

-- Step 6: Identify completely new records (not existing in target)
, new_records as (
  select
    s.*
  from source_data_deduped s
  left join {{ this }} t
    on s.src_id = t.src_id
  where t.src_id is null -- Only records not found in target
    and s._ab_cdc_deleted_at is null  -- Exclude records that are already deleted
)

-- Step 7: Detect deleted records (exist in target but not in source)
, deleted_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.num_facture,
    t.facture_id,
    t.exercice_id,
    t.p_eau_id,
    t.usagers_id,
    t.date_fact,
    t.montant_anterieur,
    t.montant_paye,
    t.montant_total,
    t.volume,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    s._ab_cdc_deleted_at as valid_to,  -- Use actual deletion timestamp from CDC
    false as is_current                 -- Mark as deleted/historical    
  from {{ this }} t
  inner join all_factures s
    on t.src_id = (s.src || '_' || s.facture_id)
  where t.is_current = true
    and s._ab_cdc_deleted_at is not null  -- Record has been deleted
)

-- Step 8: Combine new records and new versions of changed records
, records_to_insert as (
  -- New records
  select * from new_records

  union all

  -- New versions of existing records that changed
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
  from changed_records
)

-- Step 9: Add SCD2 metadata columns to records being inserted
, final_inserts as (
  select
    {{ dbt_utils.surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key, -- Unique version key (generate_surrogate_key in dbt_utils with higher version | surrogate_key for this version0.8..)
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
    _ab_cdc_updated_at as valid_from, -- Use actual CDC timestamp when change occurred
    null::timestamp as valid_to,      -- NULL = current record
    true as is_current                -- Mark as active version
  from records_to_insert
)

-- Step 10: Return both expired records and new inserts
select * from expired_records -- Historical versions with end dates
union all
select * from deleted_records
union all
select * from final_inserts   -- New current versions

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
  date_fact,
  montant_anterieur,
  montant_paye,
  montant_total,
  volume,
  src,
  _ab_cdc_updated_at,
  _ab_cdc_updated_at as valid_from, -- Set start date
  null::timestamp as valid_to,      -- No end date (current)
  true as is_current                -- All records are current on initial load
from source_data_deduped

{% endif %}