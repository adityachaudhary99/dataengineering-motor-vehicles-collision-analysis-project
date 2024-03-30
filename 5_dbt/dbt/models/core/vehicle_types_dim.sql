-- models/vehicle_types_dim.sql
{{ config(
    materialized='table',
    description='Types of vehicles involved in accidents.'
) }}

select
    collision_id,
    vehicle_type
from {{ ref('staging_accidents') }}
