{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id',
    schema = 'bronze'
    )
}}

with raw_source as (
    select * 
    from {{ source('raw','raw_transaction') }}
    {%- if is_incremental() %}
    where  >= (select max(TransactionDate) from {{ this }})
    {%- endif %}
),
deduped as (
    select *,
    row_number() over (partition by TransactionID order by TransactionDate desc) as row_num
    from raw_source
),
cleaned as (
    select
        cast(TransactionID as string)           as transaction_id,
        cast(UserID as string)                  as user_id,
        cast(MerchantID as string)               as merchant_id,
        cast(DeviceID as string)                 as device_id,
        cast(TransactionAmount as decimal(12,2)) as transaction_amount,
        cast(TransactionDate as timestamp)       as transaction_date,
        upper(trim(PaymentMethod))               as payment_method,
        upper(trim(DeviceType))                  as device_type,
        IP_Address                               as ip_address,
        cast(LocationLat as double)              as location_lat,
        cast(LocationLon as double)              as location_lon,
        IP_Country                               as ip_country,
        BillingCountry                           as billing_country,
        cast(IsFraud as boolean)                 as is_fraud_label
        
    from deduped
    where row_num = 1 and TransactionID is not null and TransactionAmount > 0

)
select * from cleaned