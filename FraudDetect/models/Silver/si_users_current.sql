{{ config(
    materialized='table',
    unique_key='user_id',
    schema='silver',
)}}

select user_id, billing_country, preferred_payment_method, dbt_valid_from
from {{ ref('snap_users_dim') }}
where dbt_valid_to is null