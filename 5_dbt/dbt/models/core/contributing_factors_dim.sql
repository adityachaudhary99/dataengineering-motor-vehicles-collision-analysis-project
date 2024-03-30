-- models/contributing_factors_dim.sql
{{ config(
    materialized='table',
    description='Contributing factors for accidents.'
) }}

select
    collision_id,
    contributing_factor
from {{ ref('staging_accidents') }}
