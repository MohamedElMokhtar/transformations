{{
  config(
    materialized='incremental'
  )
}}

select Id from {{ source('public', 'ahs_communes') }}
