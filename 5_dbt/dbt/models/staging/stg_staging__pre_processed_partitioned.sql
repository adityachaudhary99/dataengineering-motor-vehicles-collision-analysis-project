{{ config(materialized='view') }}

with 

source as (

    select * from {{ source('staging', 'pre_processed_partitioned') }}

),

renamed as (

    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['collision_id', 'timestamp']) }} as collision_key,    
        {{ dbt.safe_cast("collision_id", api.Column.translate_type("integer")) }} as collision_id,
        {{ dbt.safe_cast("zip_code", api.Column.translate_type("integer")) }} as zip_code,
        {{ title_case('borough') }} as borough,
        {{ dbt.safe_cast("street_name", api.Column.translate_type("string")) }} as street_name,

        -- timestamps, time, date
        timestamp as crash_timestamp,
        crash_date,
        CAST(crash_time AS TIME) as crash_time,

        -- collisions info
        {{ dbt.safe_cast("persons_injured", api.Column.translate_type("integer")) }} as persons_injured,
        {{ dbt.safe_cast("persons_killed", api.Column.translate_type("integer")) }} as persons_killed,
        {{ dbt.safe_cast("pedestrians_injured", api.Column.translate_type("integer")) }} as pedestrians_injured,
        {{ dbt.safe_cast("pedestrians_killed", api.Column.translate_type("integer")) }} as pedestrians_killed,
        {{ dbt.safe_cast("cyclists_injured", api.Column.translate_type("integer")) }} as cyclists_injured,
        {{ dbt.safe_cast("cyclists_killed", api.Column.translate_type("integer")) }} as cyclists_killed,
        {{ dbt.safe_cast("motorists_injured", api.Column.translate_type("integer")) }} as motorists_injured,
        {{ dbt.safe_cast("motorists_killed", api.Column.translate_type("integer")) }} as motorists_killed,
        {{ dbt.safe_cast("contributing_factor", api.Column.translate_type("string")) }} as contributing_factor,
        {{ dbt.safe_cast("vehicle_type", api.Column.translate_type("string")) }} as vehicle_type,


    from source

)

select * from renamed

-- dbt build --select <model.sql> --vars '{'is_test_run':'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}