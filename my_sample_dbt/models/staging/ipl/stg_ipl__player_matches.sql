with source as (
    select * from {{ source('kaggle', 'player_matches') }}
),

renamed as (
    select
        match_id,
        player_id,
        team_id,
        is_keeper,
        is_captain
    from source
)

select * from renamed