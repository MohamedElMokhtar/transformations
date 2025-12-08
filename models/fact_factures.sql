{{
    config(
        materialized='incremental',
        unique_key='surrogate_key'
    )
}}

with all_factures as (
    select 
        numero_facture          as num_facture,
        id                      as facture_id,
        exercice_id,
        p_eau_id,
        unite_id                as usagers_id,
        date_facture as date_fact,
        montant_sold            as montant_anterieur,
        montant_net             as montant_paye,
        montant_brute           as montant_total,
        volume,
        ancien_index,
        nouveau_index,
        'AHS' as src
    from {{ source('public', 'ahs_historique') }}

    union all

    select  
        numero_facture          as num_facture,
        id                      as facture_id,
        exercice_id,
        p_eau_id,
        unite_id                as usagers_id,
        date_facture as date_fact,
        montant_sold            as montant_anterieur,
        montant_net             as montant_paye,
        montant_brute           as montant_total,
        volume,
        ancien_index,
        nouveau_index,
        'CSM' as src
    from {{ source('public', 'csm_historique') }}
),

with_src_id as (
    select
        *,
        src || '_' || id as src_id,
        _ab_cdc_updated_at as valid_from
    from all_factures
    {% if is_incremental() %}
    where _ab_cdc_updated_at > (select max(valid_from) from {{ this }})
    {% endif %}
),

date_dim as (
    select
        date_key,
        date_facture
    from {{ ref('dim_date') }}
),

{% if is_incremental() %}
-- Get existing records that need to be closed
existing_records as (
    select
        e.*
    from {{ this }} e
    inner join with_src_id n
        on e.src_id = n.src_id
    where e.is_current = true
),

-- Close out existing current records
closed_records as (
    select
        e.*,
        n.valid_from as valid_to,
        false as is_current
    from existing_records e
    inner join with_src_id n
        on e.src_id = n.src_id
),
{% endif %}

-- New records with SCD2 columns
new_records as (
    select
        w.*,
        d.date_key as date_facture_key,
        w.valid_from,
        null as valid_to,
        true as is_current,
        {{ dbt_utils.generate_surrogate_key(['w.src_id', 'w.valid_from']) }} as surrogate_key
    from with_src_id w
    left join date_dim d
        on w.date_factures = d.date_facture
)

{% if is_incremental() %}
-- Combine closed records and new records
select * from closed_records
union all
select * from new_records
{% else %}
-- Initial load - just new records
select * from new_records
{% endif %}