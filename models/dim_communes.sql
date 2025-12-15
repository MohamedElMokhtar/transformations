{{
  config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
  )
}}

/*
  SCD2 Implementation for communes
  
  This model tracks historical changes to facture records from multiple sources.
  It maintains a complete history by creating new rows for each change, with validity dates
  and is_current flag to identify active records.
  
  Key Features:
  - Combines data from AHS,CSM,CZ,OCC,SAHARA communes sources
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


-- Step 1: Collect all communes from all RAW source tables
with all_communes as (
  select
    "Id" as commune_id,
    wilaya_id,
    designation_latin as commune,
    code_com as code_commune,
    code_wilaya,
    'AHS' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at, -- CDC timestamp for tracking changes
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at  -- CDC timestamp for deletions
  from {{ source('public', 'ahs_communes') }}
  
  union all
  
  select
    "Id",
    wilaya_id,
    designation_latin,
    code_com,
    code_wilaya,
    'CSM' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('public', 'csm_communes') }}
  
  union all
  
  select
    "Id",
    wilaya_id,
    designation_latin,
    code_com,
    code_wilaya,
    'OCC' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('public', 'occ_communes') }}
  
  union all
  
  select
    "Id",
    wilaya_id,
    designation_latin,
    code_com,
    code_wilaya,
    'CZ' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('public', 'cz_communes') }}
  
  union all
  
  select
    "Id",
    wilaya_id,
    designation_latin,
    code_com,
    code_wilaya,
    'SAHARA' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('public', 'sahara_communes') }}
),

-- Step 2: Prepare source data with natural key (src_id) and apply incremental filter
source_data as (
  select
    src || '_' || commune_id as src_id,
    commune_id,
    wilaya_id,
    commune,
    code_commune,
    code_wilaya,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    row_number() over (partition by src || '_' || commune_id order by _ab_cdc_updated_at desc) as rn  -- ADDED
  from all_communes
  {% if is_incremental() %}
  -- Only process records that have been updated since last run
  where _ab_cdc_updated_at > (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
),

-- Dedup (in case a row is added then modified before syncing)
source_data_deduped as (  -- NEW CTE
  select
    src_id,
    commune_id,
    wilaya_id,
    commune,
    code_commune,
    code_wilaya,
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
    s.commune_id,
    s.wilaya_id,
    s.commune,
    s.code_commune,
    s.code_wilaya,
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
    (s.commune_id is distinct from t.commune_id)
    or (s.wilaya_id is distinct from t.wilaya_id)
    or (s.commune is distinct from t.commune)
    or (s.code_commune is distinct from t.code_commune)
    or (s.code_wilaya is distinct from t.code_wilaya)
)

-- Step 5: Expire old versions of changed records by setting end date and is_current flag
, expired_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.commune_id,
    t.wilaya_id,
    t.commune,
    t.code_commune,
    t.code_wilaya,
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
    t.commune_id,
    t.wilaya_id,
    t.commune,
    t.code_commune,
    t.code_wilaya,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    s._ab_cdc_deleted_at as valid_to,  -- Use actual deletion timestamp from CDC
    false as is_current                 -- Mark as deleted/historical    
  from {{ this }} t
  inner join all_communes s
    on t.src_id = (s.src || '_' || s.commune_id)
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
    commune_id,
    wilaya_id,
    commune,
    code_commune,
    code_wilaya,
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
    commune_id,
    wilaya_id,
    commune,
    code_commune,
    code_wilaya,
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
  commune_id,
  wilaya_id,
  commune,
  code_commune,
  code_wilaya,
  src,
  _ab_cdc_updated_at,
  _ab_cdc_updated_at as valid_from, -- Set start date
  null::timestamp as valid_to,      -- No end date (current)
  true as is_current                -- All records are current on initial load
from source_data_deduped

{% endif %}