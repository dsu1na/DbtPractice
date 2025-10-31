with source as (
    select * from {{ source('kaggle', 'players') }}
),

renamed as (
    select
        player_id,
        player_name,
        dob AS date_of_birth,
        batting_hand,
        bowling_skill,
        country,
        is_umpire
    from source
)

select * from renamed