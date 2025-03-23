with source_data as(
    select 
        order_id,
        user_id,
        item_id,
        num_of_items,
        discount,
        order_placed_date,
        created_date
    from {{ source('bronze_tables','orders_bronze')}}
)

select *,
    CURRENT_TIMESTAMP() as updated_date
from source_data