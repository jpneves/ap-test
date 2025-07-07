{{
  config(
    materialized = 'incremental',
    unique_key = ['invoice_key'],
    incremental_strategy='merge',
    merge_exclude_columns = ['created_timestamp'],
    on_schema_change='append_new_columns',
  )

}}

{% set sk_keys = ['invoice_id', 'data_source'] %}

with 
kaggle_invoice as (

    select 
        {{ dbt_utils.generate_surrogate_key(sk_keys) }} as invoice_key,
        created_timestamp as updated_timestamp,
        * 
    from {{ ref('stg_kaggle__invoice') }}

), 

customer as (

    select
        customer_key, 
        customer_id,
        data_source
    from {{ ref('customer') }}

)

select
    i.invoice_key,
    i.invoice_id,
    c.customer_key,
    i.invoice_date,
    coalesce(i.billing_address,'Unkown') as billing_address,
    coalesce(i.billing_city,'Unkown') as billing_city,
    coalesce(i.billing_state,'Unkown') as billing_state,
    coalesce(i.billing_country,'Unkown') as billing_country,
    coalesce(i.billing_postal_code,'Unkown') as billing_postal_code,
    i.total_amount,
    i.created_timestamp,
    i.updated_timestamp,
    i.data_source

from kaggle_invoice i

left join customer c
on i.customer_id = c.customer_id
and i.data_source = c.data_source

--remove invoices with errors
where i.total_amount > 0
and i.invoice_date is not null

{% if is_incremental() %}

  and updated_timestamp > (select max(updated_timestamp) from {{ this }} )
  
{% endif %}
