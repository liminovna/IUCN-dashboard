-- replace
CREATE TABLE IF NOT EXISTS datamart.changes__datamart (
    `assessment_id`                UInt64,
    `assessment_date`              Nullable(DateTime),
    `year_published`               UInt16,
    `latest`                       Nullable(Bool),
    `url`                          Nullable(String),
    `possibly_extinct`             Nullable(Bool),
    `possibly_extinct_in_the_wild` Nullable(Bool),
    `scientific_name`              String,
    `kingdom`                      String,
    `population_trend`             Nullable(String),
    `population_trend_code`        Nullable(Float32),
    `red_list_category_version`    Nullable(Float32),
    `red_list_category`            Nullable(String),
    `red_list_category_code`       Nullable(String),
    `category_sorted`              Nullable(String),
    `prev_category_sorted`         Nullable(String),
    `n_assessments`                UInt64,
    `assessment_rank`              UInt64,
    `systems`                      Array(String),
    `habitats`                     Array(String),
    `CREATE_DTTM`                  DateTime default now()
);

TRUNCATE TABLE datamart.changes__datamart;

INSERT INTO datamart.changes__datamart (* EXCEPT(`CREATE_DTTM`))
with assessments_counts as (
    select scientific_name, kingdom, count() as n_assessments
    from detail.assessments a
    left join detail.taxonomy t using scientific_name
    group by 1,2
),
system_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(system) AS systems
    FROM detail.systems
    GROUP BY assessment_id
),
habitat_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(m.habitat_lvl1) AS habitats
    FROM detail.habitats ha
    left join dict.habitat_system_mapping m on m.habitat_code = ha.code
    GROUP BY assessment_id
),
previous_assessments as (
    select 
        assessment_id,
        lag(red_list_category_code) over (partition by scientific_name order by assessment_date, year_published) as prev_red_list_category_code
    from detail.assessments
)
select
a.assessment_id,
a.assessment_date,
a.year_published,
a.latest,
a.url,
a.possibly_extinct,
a.possibly_extinct_in_the_wild,
a.scientific_name as scientific_name,
a.kingdom,
a.population_trend,
a.population_trend_code,
a.red_list_category_version,
a.red_list_category,
a.red_list_category_code,
rl.category_sorted,
prev_rl.category_sorted as prev_category_sorted,
c.n_assessments,
row_number() over (partition by scientific_name order by assessment_date asc) as assessment_rank,
coalesce(sa.systems, []) AS systems,
coalesce(ha.habitats, []) AS habitats
from assessments_counts c
left join detail.assessments a using scientific_name
left join dict.red_list_categories_mapping rl on a.red_list_category_code = rl.code and rl.version = '3.1'
left join previous_assessments prev_a on a.assessment_id = prev_a.assessment_id
left join dict.red_list_categories_mapping prev_rl on prev_a.prev_red_list_category_code = prev_rl.code and prev_rl.version = '3.1'
LEFT JOIN system_arrays AS sa
    ON a.assessment_id = sa.assessment_id
LEFT JOIN habitat_arrays AS ha
    ON a.assessment_id = ha.assessment_id;