{{
  config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
  )
}}

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
    ancien_index,
    nouveau_index,
    'AHS' as src,
    _ab_cdc_updated_at
  from {{ source('public', 'ahs_factures') }}
  
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
    ancien_index,
    nouveau_index,
    'CSM' as src,
    _ab_cdc_updated_at
  from {{ source('public', 'csm_factures') }}
),

all_factures_with_dates as (
  select
    a.*,
    d.full_date as date_facture
  from all_factures a
  left join {{ ref('dim_date') }} d on a.date_fact = d.full_date
),

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
    ancien_index,
    nouveau_index,
    src,
    _ab_cdc_updated_at,
    date_facture
  from all_factures_with_dates
  {% if is_incremental() %}
  where _ab_cdc_updated_at > (select max(_ab_cdc_updated_at) from {{ this }})
  {% endif %}
)

{% if is_incremental() %}

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
    s.ancien_index,
    s.nouveau_index,
    s.src,
    s._ab_cdc_updated_at,
    s.date_facture
  from source_data s
  inner join {{ this }} t
    on s.src_id = t.src_id
    and t.is_current = true
  where
    (s.num_facture is distinct from t.num_facture)
    or (s.exercice_id is distinct from t.exercice_id)
    or (s.p_eau_id is distinct from t.p_eau_id)
    or (s.usagers_id is distinct from t.usagers_id)
    or (s.date_facture is distinct from t.date_facture)
    or (s.montant_anterieur is distinct from t.montant_anterieur)
    or (s.montant_paye is distinct from t.montant_paye)
    or (s.montant_total is distinct from t.montant_total)
    or (s.volume is distinct from t.volume)
    or (s.ancien_index is distinct from t.ancien_index)
    or (s.nouveau_index is distinct from t.nouveau_index)
)

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
    t.ancien_index,
    t.nouveau_index,
    t.src,
    t._ab_cdc_updated_at,
    t.date_facture,
    t.valid_from,
    current_timestamp as valid_to,
    false as is_current
  from {{ this }} t
  inner join changed_records c
    on t.src_id = c.src_id
  where t.is_current = true
)

, new_records as (
  select
    s.*
  from source_data s
  left join {{ this }} t
    on s.src_id = t.src_id
  where t.src_id is null
)

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
    date_fact,
    montant_anterieur,
    montant_paye,
    montant_total,
    volume,
    ancien_index,
    nouveau_index,
    src,
    _ab_cdc_updated_at,
    date_facture
  from changed_records
)

, final_inserts as (
  select
    {{ dbt_utils.generate_surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key,
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
    ancien_index,
    nouveau_index,
    src,
    _ab_cdc_updated_at,
    date_facture,
    current_timestamp as valid_from,
    null::timestamp as valid_to,
    true as is_current
  from records_to_insert
)

select * from expired_records
union all
select * from final_inserts

{% else %}

select
  {{ dbt_utils.generate_surrogate_key(['src_id', '_ab_cdc_updated_at']) }} as surrogate_key,
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
  ancien_index,
  nouveau_index,
  src,
  _ab_cdc_updated_at,
  date_facture,
  current_timestamp as valid_from,
  null::timestamp as valid_to,
  true as is_current
from source_data

{% endif %}