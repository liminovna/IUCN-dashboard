-- replace
CREATE TABLE IF NOT EXISTS datamart.changes__datamart (
    `assessment_id`                UInt64,
    `assessment_date`              DateTime,
    `year_published`               UInt16,
    `latest`                       Bool,
    `url`                          String,
    `possibly_extinct`             Bool,
    `possibly_extinct_in_the_wild` Bool,
    `scientific_name`              String,
    `kingdom`                      String,
    `population_trend`             LowCardinality(String) default 'empty',
    `population_trend_code`        Float32 default 0.0,
    `red_list_category_version`    LowCardinality(String) default 'empty',
    `red_list_category`            LowCardinality(String) default 'empty',
    `red_list_category_code`       LowCardinality(String) default 'empty',
    `category_sorted`              LowCardinality(String) default 'empty',
    `prev_category_sorted`         Nullable(String),
    `n_assessments`                UInt64,
    `assessment_rank`              UInt64,
    `CREATE_DTTM`                  DateTime default now()
)
ORDER BY (kingdom, population_trend, category_sorted);

TRUNCATE TABLE datamart.changes__datamart;

INSERT INTO datamart.changes__datamart (* EXCEPT(`CREATE_DTTM`))
WITH assessments_counts as (
    select t.scientific_name as scientific_name, t.kingdom_name as kingdom_name, count() as n_assessments
    from detail.assessments a
    left join detail.taxonomy t using scientific_name
    group by 1,2
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
c.kingdom_name as kingdom,
a.population_trend,
a.population_trend_code,
a.red_list_category_version,
a.red_list_category,
a.red_list_category_code,
rl.category_sorted,
prev_rl.category_sorted as prev_category_sorted,
c.n_assessments,
row_number() over (partition by scientific_name order by assessment_date asc) as assessment_rank
from assessments_counts c
left join detail.assessments a using scientific_name
left join dict.red_list_categories_mapping rl on a.red_list_category_code = rl.code and rl.version = '3.1'
left join previous_assessments prev_a on a.assessment_id = prev_a.assessment_id
left join dict.red_list_categories_mapping prev_rl on prev_a.prev_red_list_category_code = prev_rl.code and prev_rl.version = '3.1'
