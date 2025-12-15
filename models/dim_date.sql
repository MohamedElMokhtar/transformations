{{ 
    config(
        materialized='incremental',
        unique_key='full_date'
    ) 
}}

/*
  This model populates dim_date with all unique dates that are in factures,payments ....
*/

-- Step 1: extract all distinct dates from raw tables
with raw_dates as (

    -- FACTURES
    select distinct date_facture as full_date
    from {{ source('__raw_', 'ahs_factures') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'csm_factures') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'cz_factures') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'occ_factures') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'sahara_factures') }}
    where _ab_cdc_deleted_at is null

    union

    -- PAYEMENTS

    select distinct date_paie as full_date
    from {{ source('__raw_', 'ahs_payements') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_paie
    from {{ source('__raw_', 'csm_payements') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_paie
    from {{ source('__raw_', 'cz_payements') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_paie
    from {{ source('__raw_', 'occ_payements') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_paie
    from {{ source('__raw_', 'sahara_payements') }}
    where _ab_cdc_deleted_at is null

    union

    -- MISE EN DEMEURE

    select distinct date_facture as full_date
    from {{ source('__raw_', 'ahs_miseen_demeure') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'csm_miseen_demeure') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'cz_miseen_demeure') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'occ_miseen_demeure') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('__raw_', 'sahara_miseen_demeure') }}
    where _ab_cdc_deleted_at is null

    union

    -- RECOURS

    select distinct date_recour as full_date
    from {{ source('__raw_', 'ahs_recours') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_recour
    from {{ source('__raw_', 'csm_recours') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_recour
    from {{ source('__raw_', 'cz_recours') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_recour
    from {{ source('__raw_', 'occ_recours') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_recour
    from {{ source('__raw_', 'sahara_recours') }}
    where _ab_cdc_deleted_at is null
),

-- Step 2: keep only dates that are not already in dim_date (incremental logic)
new_dates as (
    select
        full_date,
        extract(day   from full_date)::int   as jour,
        extract(month from full_date)::int   as mois,
        extract(year  from full_date)::int   as annee
    from raw_dates
    {% if is_incremental() %}
    where full_date not in (select full_date from {{ this }})
    {% endif %}
)

-- Step 3: assign incremental surrogate key for NEW rows only
select
    full_date,
    jour,
    mois,
    annee,

    -- if incremental mode: append new keys after existing max(date_id)
    {% if is_incremental() %}
        (select coalesce(max(date_id), 0) from {{ this }}) 
        + row_number() over (order by full_date) 
        as date_id
    {% else %}
        -- initial load: generate date_id from 1..N
        row_number() over (order by full_date) as date_id
    {% endif %}

from new_dates
order by full_date