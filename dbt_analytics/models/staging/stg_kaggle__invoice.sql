with source as (

    select * 
    from {{ source('kaggle', 'invoice') }}

),

invoice as (

select
    nullif("InvoiceId"::varchar,'')  as invoice_id,
    nullif("CustomerId"::varchar,'')::varchar as customer_id,
    nullif("InvoiceDate"::varchar,'')::date as invoice_date,
    initcap(nullif("BillingAddress"::varchar,'')) as billing_address,
    initcap(nullif("BillingCity"::varchar,'')) as billing_city,
    upper(nullif("BillingState"::varchar,'')) as billing_state,
    initcap(nullif("BillingCountry"::varchar,'')) as billing_country,
    nullif("BillingPostalCode"::varchar,'') as billing_postal_code,
    cast("Total" as decimal(10,2)) as total_amount,
    "IngestionTimestamp"::timestamp as created_timestamp,
    "DataSource" as data_source

from source

), 

dedup as (

    select 
        *,
        row_number() over (partition by invoice_id order by created_timestamp desc) as seq

    from invoice

    where created_timestamp::date between '{{ var("start_date") }}' and '{{ var("end_date") }}'

)

select * from dedup
where seq = 1
