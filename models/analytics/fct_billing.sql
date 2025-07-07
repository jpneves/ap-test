{{ config(
    materialized='table', 
    unique_key=['customer_key', 'date_key', 'location_key']
    )
}}

{% set location_keys = ['billing_city', 'billing_state', 'billing_country', 'billing_postal_code'] %}

select
    i.customer_key,
    i.invoice_date as date_key, --dimension 'date' would be helpful
    {{ dbt_utils.generate_surrogate_key(location_keys) }} as location_key,--dimension 'location' would be helpful
    sum(i.total_amount) as total_invoice_amount,
    count(distinct invoice_key) as total_invoice_qty,
    count(distinct case when i.total_amount > 100 then invoice_key end) as high_value_invoice_qty,
    count(distinct case when i.total_amount < 5 then invoice_key end) as low_value_invoice_qty,
    avg(i.total_amount) as avg_invoice_amount,
    min(i.total_amount) as min_invoice_amount,
    max(i.total_amount) as max_invoice_amount,
    current_timestamp as updated_timestamp

from {{ ref('invoice') }} i

where 1=1

group by 1,2,3