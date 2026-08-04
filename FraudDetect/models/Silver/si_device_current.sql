{{config(
    materialized='table',
    unique_key='device_id',
    schema='silver',
)}}


select device_id, device_type, primary_user_id, dbt_valid_from
from {{ ref('snap_device_dim') }}
where dbt_valid_to is null