{{ config(tags=['silver_stage_3', 'fixture', 'players_current'], severity='error') }}

with countries(country_id, country_name) as (
    select * from values
        (1, 'France'),
        (2, 'Senegal'),
        (3, 'Congo'),
        (4, 'Congo'),
        (5, 'Türkiye')
),
aliases(alias_name, canonical_country_name) as (
    select * from values
        ('Turkey', 'Türkiye'),
        ('DualAlias', 'France'),
        ('DualAlias', 'Senegal')
),
players(player_id, team_id, birth_name, citizenship_name) as (
    select * from values
        (101, null, 'France', 'Senegal'),
        (102, 10, 'Nowhere', 'Turkey'),
        (103, null, 'Congo', 'DualAlias')
),
teams(team_id, country_id) as (
    select * from values (10, 999)
),
country_candidates as (
    select
        upper(country_name) as country_key,
        count(*) as candidate_count,
        min(country_id) as candidate_id
    from countries
    group by 1
),
alias_candidates as (
    select
        upper(aliases.alias_name) as country_key,
        count(*) as alias_definition_count,
        count(countries.country_id) as target_candidate_count,
        min(countries.country_id) as candidate_id
    from aliases
    left join countries
        on upper(aliases.canonical_country_name) = upper(countries.country_name)
    group by 1
),
observed_keys as (
    select upper(birth_name) as country_key from players
    union
    select upper(citizenship_name) as country_key from players
),
resolution as (
    select
        observed.country_key,
        case
            when coalesce(direct.candidate_count, 0) > 1
              or coalesce(alias_definition_count, 0) > 1
              or coalesce(target_candidate_count, 0) > 1 then null
            when direct.candidate_count = 1 then direct.candidate_id
            when alias_definition_count = 1 and target_candidate_count = 1
                then aliases.candidate_id
        end as resolved_country_id,
        case
            when coalesce(direct.candidate_count, 0) > 1
              or coalesce(alias_definition_count, 0) > 1
              or coalesce(target_candidate_count, 0) > 1 then 'ambiguous'
            when direct.candidate_count = 1 then 'matched_name'
            when alias_definition_count = 1 and target_candidate_count = 1
                then 'matched_alias'
            else 'not_found'
        end as resolution_status
    from observed_keys as observed
    left join country_candidates as direct using (country_key)
    left join alias_candidates as aliases using (country_key)
),
profiles as (
    select
        players.player_id,
        teams.team_id as resolved_team_id,
        team_country.country_id as team_country_id,
        birth.resolved_country_id as birth_country_id,
        birth.resolution_status as birth_status,
        citizenship.resolved_country_id as citizenship_country_id,
        citizenship.resolution_status as citizenship_status
    from players
    left join teams on players.team_id = teams.team_id
    left join countries as team_country on teams.country_id = team_country.country_id
    left join resolution as birth on upper(players.birth_name) = birth.country_key
    left join resolution as citizenship on upper(players.citizenship_name) = citizenship.country_key
),
actual as (
    select
        count(*) as row_count,
        count(distinct player_id) as distinct_players,
        count_if(player_id = 101 and resolved_team_id is null
            and birth_country_id = 1 and citizenship_country_id = 2) as no_team_and_distinct_roles,
        count_if(player_id = 102 and resolved_team_id = 10
            and team_country_id is null and citizenship_country_id = 5
            and citizenship_status = 'matched_alias') as absent_team_country_and_alias,
        count_if(player_id = 103 and birth_country_id is null
            and birth_status = 'ambiguous' and citizenship_country_id is null
            and citizenship_status = 'ambiguous') as ambiguous_left_unresolved
    from profiles
)
select *
from actual
where row_count <> 3
   or distinct_players <> 3
   or no_team_and_distinct_roles <> 1
   or absent_team_country_and_alias <> 1
   or ambiguous_left_unresolved <> 1

