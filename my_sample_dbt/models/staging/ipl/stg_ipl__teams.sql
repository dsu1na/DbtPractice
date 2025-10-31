with source as (
    select * from {{ source('kaggle', 'teams') }}
)

select * from source