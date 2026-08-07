---lets built the base line for customer behaviour 

{{ config(
    materialized='incremental',
    unique_key='user_id',
    schema='gold',
    incremental_strategy='merge',
)
}}

-- looking for the latest transactions 

with new_transaction as (
    select *
    from {{ ref('si_transaction_fact') }}

    {% if is_incremental() %}
    where transaction_date > (
        select coalesce(max(last_updated_at), '1900-01-01') from {{ this }}
    )
    {% endif %}

),
--finding the distinct users from the transaction table
affected_users as (
    select distinct user_id from new_transaction
),

--pulling each affected user's FULL trailing 30-day window, not just
-- today's rows, so the rolling average/stddev stay mathematically correct

window_data as (
    select f.*
    from {{ ref('si_transaction_fact')}} as f
    inner join affected_users as a
    on f.user_id = a.user_id
    where f.transaction_date >= dateadd(day,-30,current_date())

),

calculated as (
    select 
        user_id,
        avg(transaction_amount)                          as avg_amount_30d,
        stddev(transaction_amount)                        as stddev_amount_30d,
        percentile_approx(transaction_amount, 0.9)        as p90_amount_30d,
        min(hour(transaction_date))                        as earliest_active_hour,
        max(hour(transaction_date))                        as latest_active_hour,
        count(*)                                            as transaction_count_30d,
        current_timestamp()                                as last_updated_at
    from window_data
    group by user_id
)

---main query 

select * 
from calculated

