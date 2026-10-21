-- replace
CREATE TABLE IF NOT EXISTS datamart.changes__datamart (
    `assessment_id`                UInt64                                   COMMENT 'ID of assessment',
    `assessment_date`              DateTime                                 COMMENT 'Date when the evaluation was finished by the assessor',
    `year_published`               UInt16                                   COMMENT 'Year when the evaluation was published',
    `latest`                       Bool                                     COMMENT 'If the assessment is recent and not historical',
    `url`                          String                                   COMMENT 'Link to the assessment on the IUCN website',
    `possibly_extinct`             Bool                                     COMMENT 'If the species is highly likely to be extinct',
    `possibly_extinct_in_the_wild` Bool                                     COMMENT 'If the species is highly likely to be extinct in the wild',
    `scientific_name`              String                                   COMMENT 'Scientific name of the species',
    `kingdom`                      String                                   COMMENT 'Biological kingdom of the species',
    `population_trend`             LowCardinality(String) default 'empty'   COMMENT 'Estimated population trend (text)',
    `population_trend_code`        Float32 default 0.0                      COMMENT 'Estimated population trend (code)',
    `red_list_category_version`    LowCardinality(String) default 'empty'   COMMENT 'Version of Red List category',
    `red_list_category`            LowCardinality(String) default 'empty'   COMMENT 'Estimated Red List category (text)',
    `red_list_category_code`       LowCardinality(String) default 'empty'   COMMENT 'Estimated Red List category (code)',
    `category_sorted`              LowCardinality(String) default 'empty'   COMMENT 'Estimated Red List category (text with added rank)',
    `prev_category_sorted`         Nullable(String)                         COMMENT 'Red List category according to the previous assessment (text with added rank)',
    `n_assessments`                UInt64                                   COMMENT 'Total number of assessments for the species',
    `assessment_rank`              UInt64                                   COMMENT 'Sequential number of the assessment',
    `CREATE_DTTM`                  DateTime default now()                   COMMENT 'Timestamp when the record was added to the table'
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
