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

-- Step 1: Collect all unites from all RAW source tables
with all_unites as (
  select
      "Id" as usagers_id,
  commune_id,
  type_unite_id as type_usagers_id,
  statut_unite_id as status_usagers_id,
  etat_unite_id as etat_usagers_id,
  type_activite_id,
  reference,
  nom_latin as nom,
  dateactive as date_activite,
  datearret as date_arret,
  adresse_latin as adresse,
    'AHS' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at, -- CDC timestamp for tracking changes
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at  -- CDC timestamp for deletions
  from {{ source('__raw_', 'ahs_unites') }}                                       -- ######################## To be changed
  
  union all
  
  select
      "Id" as usagers_id,
  commune_id,
  type_unite_id as type_usagers_id,
  statut_unite_id as status_usagers_id,
  etat_unite_id as etat_usagers_id,
  type_activite_id,
  reference,
  nom_latin as nom,
  dateactive as date_activite,
  datearret as date_arret,
  adresse_latin as adresse,
    'CSM' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'csm_unites') }}                                       -- ######################## To be changed
  
  union all
  
  select
     "Id" as usagers_id,
  commune_id,
  type_unite_id as type_usagers_id,
  statut_unite_id as status_usagers_id,
  etat_unite_id as etat_usagers_id,
  type_activite_id,
  reference,
  nom_latin as nom,
  dateactive as date_activite,
  datearret as date_arret,
  adresse_latin as adresse,
    'OCC' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'occ_unites') }}                                       -- ######################## To be changed
  
  union all
  
  select
     "Id" as usagers_id,
  commune_id,
  type_unite_id as type_usagers_id,
  statut_unite_id as status_usagers_id,
  etat_unite_id as etat_usagers_id,
  type_activite_id,
  reference,
  nom_latin as nom,
  dateactive as date_activite,
  datearret as date_arret,
  adresse_latin as adresse,
    'CZ' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'cz_unites') }}                                       -- ######################## To be changed
  
  union all
  
  select
     "Id" as usagers_id,
  commune_id,
  type_unite_id as type_usagers_id,
  statut_unite_id as status_usagers_id,
  etat_unite_id as etat_usagers_id,
  type_activite_id,
  reference,
  nom_latin as nom,
  dateactive as date_activite,
  datearret as date_arret,
  adresse_latin as adresse,
    'SAHARA' as src,
    _ab_cdc_updated_at::timestamp as _ab_cdc_updated_at,
    _ab_cdc_deleted_at::timestamp as _ab_cdc_deleted_at
  from {{ source('__raw_', 'sahara_unites') }}                                       -- ######################## To be changed
),

-- Step 2: Prepare source data with natural key (src_id) and apply incremental filter
source_data as (
  select
    src || '_' || usagers_id as src_id,
   usagers_id,
commune_id,
type_usagers_id,
status_usagers_id,
etat_usagers_id,
type_activite_id,
reference,
nom,
date_activite,
date_arret,
adresse,
    src,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    row_number() over (partition by src || '_' || usagers_id order by _ab_cdc_updated_at desc) as rn  -- ADDED
  from all_unites
  {% if is_incremental() %}
  -- Only process records that have been updated since last run
  where _ab_cdc_updated_at > (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
),

-- Dedup (in case a row is added then modified before syncing)
source_data_deduped as (  -- NEW CTE
  select
    src_id,
    usagers_id,
commune_id,
type_usagers_id,
status_usagers_id,
etat_usagers_id,
type_activite_id,
reference,
nom,
date_activite,
date_arret,
adresse,
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
    s.usagers_id,
s.commune_id,
s.type_usagers_id,
s.status_usagers_id,
s.etat_usagers_id,
s.type_activite_id,
s.reference,
s.nom,
s.date_activite,
s.date_arret,
s.adresse,
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
    (s.etat_usagers_id is distinct from t.etat_usagers_id)                              -- ######################## To be changed THIS PART you put only the data (don't put id)
    or (s.commune_id is distinct from t.commune_id)
    or (s.status_usagers_id is distinct from t.status_usagers_id)
    or (s.type_activite_id is distinct from t.type_activite_id)
    or (s.reference is distinct from t.reference)
    or (s.nom is distinct from t.nom)
    or (s.date_activite is distinct from t.date_activite)  
    or (s.date_arret is distinct from t.date_arret) 
    or (s.adresse is distinct from t.adresse)        -- ######################## To be changed
)

-- Step 5: Expire old versions of changed records by setting end date and is_current flag
, expired_records as (
  select
    t.surrogate_key,
    t.src_id,
    t.usagers_id,
t.commune_id,
t.type_usagers_id,
t.status_usagers_id,
t.etat_usagers_id,
t.type_activite_id,
t.reference,
t.nom,
t.date_activite,
t.date_arret,
  case 
    when t.date_arret is null then 'actif'
    else 'inactif'
  end as t.etat,
t.adresse,
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
    t.usagers_id,
t.commune_id,
t.type_usagers_id,
t.status_usagers_id,
t.etat_usagers_id,
t.type_activite_id,
t.reference,
t.nom,
t.date_activite,
t.date_arret,
  case 
    when t.date_arret is null then 'actif'
    else 'inactif'
  end as t.etat,
t.adresse,
    t.src,
    t._ab_cdc_updated_at,
    t.valid_from,
    s._ab_cdc_deleted_at as valid_to,  -- Use actual deletion timestamp from CDC
    false as is_current                 -- Mark as deleted/historical    
  from {{ this }} t
  inner join all_unites s
    on t.src_id = (s.src || '_' || s.usagers_id)
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
   usagers_id,
commune_id,
type_usagers_id,
status_usagers_id,
etat_usagers_id,
type_activite_id,
reference,
nom,
date_activite,
date_arret,
adresse,
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
    usagers_id,
commune_id,
type_usagers_id,
status_usagers_id,
etat_usagers_id,
type_activite_id,
reference,
nom,
date_activite,
date_arret,
  case 
    when date_arret is null then 'actif'
    else 'inactif'
  end as etat,
adresse,
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
  usagers_id,
commune_id,
type_usagers_id,
status_usagers_id,
etat_usagers_id,
type_activite_id,
reference,
nom,
date_activite,
date_arret,
  case 
    when date_arret is null then 'actif'
    else 'inactif'
  end as etat,
adresse,
  src,
  _ab_cdc_updated_at,
  _ab_cdc_updated_at as valid_from, -- Set start date
  null::timestamp as valid_to,      -- No end date (current)
  true as is_current                -- All records are current on initial load
from source_data_deduped

{% endif %}