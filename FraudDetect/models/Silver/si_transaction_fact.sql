{{
  config(
    materialized = 'incremental',
    unique_id='transaction_id',
    incremental_strategy='merge',
    schema='silver',
    )
}}

select 

    transaction_id, user_id, merchant_id, device_id, transaction_amount,
    transaction_date, payment_method, device_type, ip_address,
    location_lat, location_lon, ip_country, billing_country, is_fraud_label
from {{ ref('transaction_b') }}

{% if is_incremental() %}
where transaction_date >(select coalesce(max(transaction_date),'1900-01-01') from {{ this }})
{% endif %}
