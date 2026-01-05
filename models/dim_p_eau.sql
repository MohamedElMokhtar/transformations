{{
  config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
  )
}}

/*
  Template for SCD2 Implementation
*/


-- ################################################
-- ################################################
-- FIRST start with adding model dim_... in schema.yml
-- THEN CHANGE "all_..." (line22) AND ".._id" (line24) WITH CTRL+SHIFT+L
-- ################################################
-- ################################################

-- Step 1: Collect all p_eau from all RAW source tables
with all_P_eau as (
  select
   id as p_eau_id,
  nature_id as nature_p_eau_id,
  etat_compteur_id,
  "type_estimation_id" as mode_estimation_id,
 unite_id as usagers_id, 
  date_installe as date_installation,
  date_arret,

    'AHS' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at, -- CDC timestamp for tracking changes
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at  -- CDC timestamp for deletions
  from {{ source('__raw_', 'ahs_p_eau') }}                                       -- ######################## To be changed
  
  union all
  
  select
    id as p_eau_id,
  nature_id as nature_p_eau_id,
  etat_compteur_id,
  "type_estimation_id" as mode_estimation_id,
 unite_id as usagers_id, 
  date_installe as date_installation,
  date_arret,

    'CSM' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'csm_p_eau') }}                                       -- ######################## To be changed
  
  union all
  
  select
   id as p_eau_id,
  case when nature_id = 5 then 7 else nature_id end as nature_p_eau_id,
  etat_compteur_id,
  "type_estimation_id" as mode_estimation_id,
 unite_id as usagers_id, 
  date_installe as date_installation,
  date_arret,

    'OCC' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'occ_p_eau') }}                                       -- ######################## To be changed
  
  union all
  
  select
   id as p_eau_id,
  nature_id as nature_p_eau_id,
  etat_compteur_id,
  "type_estimation_id" as mode_estimation_id,
 unite_id as usagers_id, 
  date_installe as date_installation,
  date_arret,

    'CZ' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'cz_p_eau') }}                                       -- ######################## To be changed
  
  union all
  
  select
   id as p_eau_id,
  nature_id as nature_p_eau_id,
  etat_compteur_id,
  "type_estimation_id" as mode_estimation_id,
 unite_id as usagers_id, 
  date_installe as date_installation,
  date_arret,

    'SAHARA' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'sahara_p_eau') }}                                       -- ######################## To be changed
),

-- Step 2: Prepare source data with natural key (src_id) and apply incremental filter
source_data as (
  select
    src || '_' || p_eau_id as src_id,
   p_eau_id,
nature_p_eau_id,
etat_compteur_id,
mode_estimation_id,
usagers_id, 
date_installation,
date_arret,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    row_number() over (partition by src || '_' || p_eau_id order by _ab_cdc_updated_at desc) as rn  -- ADDED
  from all_P_eau
  {% if is_incremental() %}
  -- Only process records that have been updated since last run
  where _ab_cdc_updated_at > (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
),

-- Dedup (in case a row is added then modified before syncing)
source_data_deduped as (  -- NEW CTE
  select
    src_id,
    p_eau_id,
nature_p_eau_id,
etat_compteur_id,
mode_estimation_id,
usagers_id, 
date_installation,
date_arret,
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
   s.p_eau_id,
s.nature_p_eau_id,
s.etat_compteur_id,
s.mode_estimation_id,
s.usagers_id, 
s.date_installation,
s.date_arret,
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
    (s.nature_p_eau_id is distinct from t.nature_p_eau_id)                              -- ######################## To be changed THIS PART you put only the data (don't put id)
    or (s.etat_compteur_id is distinct from t.etat_compteur_id)
    or (s.mode_estimation_id is distinct from t.mode_estimation_id) 
    or (s.usagers_id is distinct from t.usagers_id) 
    or (s.date_installation is distinct from t.date_installation)
    or (s.date_arret is distinct from t.date_arret)           -- ######################## To be changed
)

-- Step 5: Expire old versions of changed records by setting end date and is_current flag
, expired_records as (
  select
    t.surrogate_key,
    t.src_id,
   t.p_eau_id,
t.nature_p_eau_id,
t.etat_compteur_id,
t.mode_estimation_id,
t.usagers_id, 
t.date_installation,
t.date_arret,
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
    t.p_eau_id,
t.nature_p_eau_id,
t.etat_compteur_id,
t.mode_estimation_id,
t.usagers_id, 
t.date_installation,
t.date_arret,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    s._ab_cdc_deleted_at as valid_to,  -- Use actual deletion timestamp from CDC
    false as is_current                 -- Mark as deleted/historical    
  from {{ this }} t
  inner join all_P_eau s
    on t.src_id = (s.src || '_' || s.p_eau_id)
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
   p_eau_id,
nature_p_eau_id,
etat_compteur_id,
mode_estimation_id,
usagers_id, 
date_installation,
date_arret,
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
   p_eau_id,
nature_p_eau_id,
etat_compteur_id,
mode_estimation_id,
usagers_id, 
date_installation,
date_arret,
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
  p_eau_id,
nature_p_eau_id,
etat_compteur_id,
mode_estimation_id,
usagers_id, 
date_installation,
date_arret,
  src,
  _ab_cdc_updated_at,
  _ab_cdc_updated_at as valid_from, -- Set start date
  null::timestamp as valid_to,      -- No end date (current)
  true as is_current                -- All records are current on initial load
from source_data_deduped

{% endif %}