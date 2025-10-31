with source as (
    select * from {{ source('kaggle', 'matches') }}
),

renamed as (
    select
        match_id,
        match_date,
        team_name_id,
        opponent_team_id,
        season_id,
        venue_name,
        toss_winner_id as toss_winner_team_id,
        toss_decision,
        is_super_over,
        is_result,
        is_duckworthlewis,
        win_type,
        CASE 
        WHEN TRIM(won_by) = "NULL" THEN NULL
        ELSE CAST(TRIM(won_by) AS INT)
        END AS won_by,

        match_winner_id,
        man_of_the_match_id,
        first_umpire_id,
        second_umpire_id,
        city_name as venue,
        host_country
    from source
)

select * from renamed