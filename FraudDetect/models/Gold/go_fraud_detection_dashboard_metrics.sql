{{
    config(
        materialized='incremental',
        unique_key='date_key',
        incremental_strategy='merge',
        schema='gold',
    )
}}

with new_predictions as (

    select *
    from {{ ref('go_fraud_prediction') }}

    {% if is_incremental() %}
    where transaction_date > (
        select coalesce(max(date_key), '1900-01-01') from {{ this }}
    )
    {% endif %}

),

daily_rollup as (

    select
        date(transaction_date) as date_key,
        count(*) as total_transactions,
        sum(case when risk_tier = 'High' then 1 else 0 end) as flagged_high,
        sum(case when risk_tier = 'Medium' then 1 else 0 end) as flagged_medium,
        sum(case when risk_tier = 'Low' then 1 else 0 end) as flagged_low,
        avg(fraud_probability) as avg_fraud_probability,
        current_timestamp() as last_updated_at
    from new_predictions
    group by date(transaction_date)

)

select * from daily_rollup