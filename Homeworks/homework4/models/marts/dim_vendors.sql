with trips_unioned as (
    select * from {{ ref('fct_trips') }}
),

vendors as (
    select distinct 
        vendor_id,
        {{ get_vendor_names('vendor_id') }} as vendor_name
    from trips_unioned
)

select * from vendors