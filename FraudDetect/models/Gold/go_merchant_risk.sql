{{
    config(
        materialized='incremental',
        unique_key='merchant_id',
        incremental_strategy='merge',
        schema='gold'
    )
}}

with new_transactions as (

    select *
    from {{ ref('si_transaction_fact') }}

    {% if is_incremental() %}
    where transaction_date > (
        select coalesce(max(last_updated_at), '1900-01-01') from {{ this }}
    )
    {% endif %}

),

affected_merchants as (
    -- only merchants with new activity since the last run get recomputed
    select distinct merchant_id
    from new_transactions

),

-- pull each affected merchant's FULL trailing 90-day window, not just
-- today's rows, so the rolling fraud rate stays mathematically correct
window_data as (

    select f.*
    from {{ ref('si_transaction_fact') }} f
    inner join affected_merchants a on f.merchant_id = a.merchant_id
    where f.transaction_date >= dateadd(day, -90, current_date())

),


aggregated as (

    select
        merchant_id,
        count(*)                                                              as transaction_count_90d,
        sum(case when is_fraud_label then 1 else 0 end)                       as fraud_count_90d,
        sum(case when is_fraud_label then 1 else 0 end) / count(*)            as fraud_rate_90d,
        avg(transaction_amount)                                               as avg_transaction_amount_90d,
        count(distinct user_id)                                               as distinct_customer_count_90d,
        current_timestamp()                                                   as last_updated_at
    from window_data
    group by merchant_id

)

select * from aggregated