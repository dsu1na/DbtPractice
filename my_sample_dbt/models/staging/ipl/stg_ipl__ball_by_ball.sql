with source as (
    select * from {{ source('kaggle', 'ball_by_ball') }}
),

renamed as (
    select
        match_id,
        innings_id,
        over_id,
        ball_id,
        team_batting_id,
        team_bowling_id,
        striker_id,
        CAST(TRIM(striker_batting_position) AS INT) AS striker_batting_position,
        CAST(TRIM(non_striker_id) AS INT) AS non_striker_id,
        CAST(TRIM(bowler_id) AS INT) AS bowler_id,

        CASE 
        WHEN TRIM(batsman_scored) = "" THEN NULL
        WHEN TRIM(batsman_scored) = "Do_nothing" THEN NULL
        ELSE CAST(TRIM(batsman_scored) AS INT)
        END AS batsman_scored,

        extra_type,

        CASE 
        WHEN TRIM(extra_runs) = "" THEN NULL
        ELSE CAST(TRIM(extra_runs) AS INT)
        END AS extra_runs,

        CASE 
        WHEN TRIM(player_dissimal_id) = "" THEN NULL
        ELSE CAST(TRIM(player_dissimal_id) AS INT)
        END AS player_dissimal_id,

        CASE 
        WHEN TRIM(fielder_id) = "" THEN NULL
        ELSE CAST(TRIM(fielder_id) AS INT)
        END AS fielder_id

    from source
)

select * from renamed