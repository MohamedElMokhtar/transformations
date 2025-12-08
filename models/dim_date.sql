{{ 
    config(
        materialized='incremental',
        unique_key='date'
    ) 
}}

-- Step 1: extract all distinct dates from raw tables
with raw_dates as (

    select distinct date_facture as date
    from {{ source('public', 'ahs_factures') }}
    where _ab_cdc_deleted_at is null

    union

    select distinct date_facture
    from {{ source('public', 'csm_factures') }}
    where _ab_cdc_deleted_at is null
),

-- Step 2: keep only dates that are not already in dim_date (incremental logic)
new_dates as (
    select
        date,
        extract(day   from date)::int   as jour,
        extract(month from date)::int   as mois,
        extract(year  from date)::int   as annee
    from raw_dates
    {% if is_incremental() %}
    where date not in (select date from {{ this }})
    {% endif %}
)

-- Step 3: assign incremental surrogate key for NEW rows only
select
    date,
    jour,
    mois,
    annee,

    -- if incremental mode: append new keys after existing max(date_id)
    {% if is_incremental() %}
        (select coalesce(max(date_id), 0) from {{ this }}) 
        + row_number() over (order by date) 
        as date_id
    {% else %}
        -- initial load: generate date_id from 1..N
        row_number() over (order by date) as date_id
    {% endif %}

from new_dates
order by date
