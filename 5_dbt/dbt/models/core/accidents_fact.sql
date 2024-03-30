-- models/accidents_fact.sql
{{ config(
    materialized='table',
    description='Aggregated accident data including the number of persons injured, killed, pedestrians injured, pedestrians killed, cyclists injured, cyclists killed, and motorists injured.'
) }}

select
    collision_id,
    crash_date,
    crash_time,
    crash_timestamp,
    sum(persons_injured) as total_persons_injured,
    sum(persons_killed) as total_persons_killed,
    sum(pedestrians_injured) as total_pedestrians_injured,
    sum(pedestrians_killed) as total_pedestrians_killed,
    sum(cyclists_injured) as total_cyclists_injured,
    sum(cyclists_killed) as total_cyclists_killed,
    sum(motorists_injured) as total_motorists_injured,
    sum(motorists_killed) as total_motorists_killed
from {{ ref('staging_accidents') }}
group by 1, 2, 3, 4
