-- models/location_dim.sql
{{ config(
    materialized='table',
    description='Location information related to accidents.'
) }}

select
    distinct zip_code,
    borough,
    street_name
from {{ ref('staging_accidents') }}
