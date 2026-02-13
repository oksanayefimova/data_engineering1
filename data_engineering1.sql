SET memory_limit='4GB';

CREATE TABLE raw_tvs as
select *
from read_json_auto('/Users/ksyusha/Downloads/tvs.json',
                    maximum_object_size=1073741824, sample_size=-1);


select * from raw_tvs limit 10;
drop table tvs_parsed;


CREATE TABLE tvs_parsed AS
SELECT
    CAST(JSON_VALUE(raw_tvs, '$.id') AS INT) AS id,
    TRIM(JSON_VALUE(raw_tvs, '$.name'), '"') AS title,
    CAST(JSON_VALUE(raw_tvs, '$.in_production') AS BOOL) AS in_production,
    TRIM(JSON_VALUE(raw_tvs, '$.status'), '"') AS status,
--    TRIM(JSON_VALUE(c.created_by, '$.name'), '"') AS creator_name,
    TRIM(JSON_VALUE(oc, '$.origin_country'), '"') AS origin_country,
    TRY_CAST(TRIM(JSON_VALUE(raw_tvs, '$.first_air_date'), '"') AS DATE) AS first_air_date,
    TRY_CAST(TRIM(JSON_VALUE(raw_tvs, '$.last_air_date'), '"') AS DATE) AS last_air_date,
--    CAST(JSON_VALUE(raw_tvs, '$.number_of_episodes') AS INT) AS number_of_episodes,
--    CAST(JSON_VALUE(raw_tvs, '$.number_of_seasons') AS INT) AS number_of_seasons,
    TRIM(JSON_VALUE(c.production_companies, '$.name'), '"') as production_company,
    CAST(JSON_VALUE(raw_tvs, '$.vote_average') AS DOUBLE) AS vote_average,
    CAST(JSON_VALUE(raw_tvs, '$.vote_count') AS INT) as vote_count,
    CAST(JSON_VALUE(raw_tvs, '$.popularity') AS DOUBLE) AS popularity,
    TRIM(JSON_VALUE(c.genres, '$.name'), '"') AS genre
FROM raw_tvs
--CROSS JOIN UNNEST(raw_tvs.created_by) AS c(created_by)
CROSS JOIN UNNEST(raw_tvs.origin_country) AS oc(origin_country)
CROSS JOIN UNNEST(raw_tvs.genres) AS c(genres)
CROSS JOIN UNNEST(raw_tvs.production_companies) AS c(production_companies)
order by popularity desc;




--- 1. TOP 3 TV-SHOWS BY POPULARITY IN EVERY COUNTRY
with cte_titles as (
select distinct
    title,
    origin_country,
    popularity
from tvs_parsed),
cte_ranked as (
    select
        title,
        origin_country,
        popularity,
        row_number() over(partition by origin_country order by popularity desc) as rn
    from cte_titles
)
select * from cte_ranked
where rn < 4;

--- 2. COUNT OF EVERY GENRE PER COUNTRY
select
    origin_country,
    genre,
    count(*) as tv_count
from tvs_parsed
group by origin_country, genre
order by origin_country;

--- 3. CHANGE OF POPULARITY OF WARNER BROS. TELEVISIONS
select production_company, count(*) as tvcount from tvs_parsed group by production_company order by tvcount desc;

with cte_creator as (select distinct title,
                          first_air_date,
                          last_air_date,
                          production_company,
                          popularity,
                          vote_average,
                          vote_count
                   from tvs_parsed
                   where production_company = 'Warner Bros. Television'
                   order by first_air_date)
select title, first_air_date, popularity, round(popularity - lag(popularity) over(order by first_air_date), 2) as pop_diff
from cte_creator
where vote_count>100;

--- 4. PERCENTAGE DIFFERENCE FROM THE AVERAGE GENRE RATE THROUGH THE US TV-SHOWS
with cte_clean as (
select distinct
    title,
    genre,
    vote_average,
    popularity,
    origin_country
from tvs_parsed
where vote_count > 100 and origin_country = 'US')
select
    title,
    genre,
    round(vote_average, 2) as rating,
    round(((vote_average/avg(vote_average) over (partition by genre))-1)*100, 2) || '%' as pct_from_avg_rate
from cte_clean
order by popularity desc;