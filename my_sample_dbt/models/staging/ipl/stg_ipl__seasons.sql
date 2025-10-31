with source as (
    select * from {{ source('kaggle', 'seasons') }}
),

renamed as (
    select
        season_id,
        season_year,
        orange_cap_id,
        purple_cap_id,
        man_of_the_series_id
    from source
)

select * from renamed