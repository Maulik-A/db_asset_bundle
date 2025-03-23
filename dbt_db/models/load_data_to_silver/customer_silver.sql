with source_data as (
    select
        user_id,
        fname,
        lname,
        date_of_birth,
        gender,
        email_address,
        contact_number,
        address,
        created_date
    from {{ source('bronze_tables','customer_bronze')}}
)

select 
    *,
    CURRENT_TIMESTAMP() AS updated_date  
from source_data