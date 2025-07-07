with source as (

    select * 
    from {{ source('kaggle', 'customer') }}

),

customer as (

    select
        nullif("CustomerId"::varchar ,'') as customer_id,
        initcap(nullif("FirstName"::varchar,'')) as first_name,
        initcap(nullif("LastName"::varchar,'')) as last_name,
        initcap(nullif("Company"::varchar,'')) as company_name,
        initcap(nullif("Address"::varchar,'')) as address,
        initcap(nullif("City"::varchar,'')) as city,
        upper(nullif("State"::varchar,'')) as state,
        initcap(nullif("Country"::varchar,'')) as country,
        nullif("PostalCode"::varchar,'') as postal_code,
        nullif("Phone"::varchar,'') as phone,
        nullif("Fax"::varchar,'') as fax,
        nullif("Email"::varchar,'') as email,
        nullif("SupportRepId"::varchar,'') as support_rep_id,
        "IngestionTimestamp"::timestamp as created_timestamp,
        "DataSource" as data_source

    from source

), 

dedup as (

    select 
        *,
        row_number() over (partition by customer_id order by created_timestamp desc) as seq

    from customer

    where created_timestamp::date between '{{ var("start_date") }}' and '{{ var("end_date") }}'

)

select * from dedup
where seq = 1

