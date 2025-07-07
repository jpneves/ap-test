{{
  config(
    materialized = 'incremental',
    unique_key = ['customer_key'],
    incremental_strategy='merge',
    merge_exclude_columns = ['created_timestamp'],
    on_schema_change='append_new_columns',
  )

}}

{% set sk_keys = ['customer_id', 'data_source'] %}

with 
kaggle_customer as (

    select 
        {{ dbt_utils.generate_surrogate_key(sk_keys) }} as customer_key,
        created_timestamp as updated_timestamp,
        * 
    from {{ ref('stg_kaggle__customer') }}

)

select
    customer_key,
    customer_id,
    first_name,
    last_name,
    company_name,
    address,
    city,
    state,
    country,
    postal_code,
    regexp_replace(phone, '[^0-9+]', '', 'g') as phone,
    regexp_replace(fax, '[^0-9+]', '', 'g') as fax,
    case when email is not null and email not like '%@%.%' then 'Invalid Format' else lower(email) end as email,
    support_rep_id,
    created_timestamp,
    updated_timestamp,
    data_source

from kaggle_customer

where 1=1

{% if is_incremental() %}

  and updated_timestamp > (select max(updated_timestamp) from {{ this }} )
  
{% endif %}
