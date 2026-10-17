--replace
CREATE TABLE IF NOT EXISTS datamart.threats__datamart
(
    `assessment_id`           UInt64,
    `assessment_date`         Nullable(DateTime),
    `scientific_name`         String,
    `year_published`          UInt16,
    `population_trend`        Nullable(String),
    `red_list_category`       Nullable(String),
    `red_list_category_code`  Nullable(String),
    `category_sorted`         Nullable(String),
    `timing`                  Nullable(String),
    `threat`                  String,
    `system`                  String,
    `habitat`                 Nullable(String),
    `CREATE_DTTM`             DateTime default now()
);

TRUNCATE TABLE datamart.threats__datamart;

-- explain analyze
INSERT INTO datamart.threats__datamart (* EXCEPT(`CREATE_DTTM`))
with latest_assessments AS
(
    SELECT
        a.assessment_id,
        a.sis_taxon_id,
        a.scientific_name,
        a.assessment_date,
        a.year_published,
        a.population_trend,
        a.population_size,
        a.red_list_category,
        a.red_list_category_code,
        rl.category_sorted
    FROM detail.assessments a
    left join dict.red_list_categories_mapping rl on rl.code = a.red_list_category_code
    WHERE a.latest = 1
    and rl.category_sorted is not null
),
SELECT distinct
    a.assessment_id as assessment_id,
    a.assessment_date,
    a.scientific_name,
    a.year_published,
    a.population_trend,
    a.red_list_category,
    a.red_list_category_code,
    a.category_sorted,
    case when th.timing = '' then Null else th.timing end as timing,
    coalesce(th_d.threat_lvl1, 'Type Unknown/Unrecorded') as threat,
    m.system as system,
    m.habitat_lvl1 as habitat
FROM latest_assessments AS a
LEFT JOIN detail.habitats AS ha ON a.assessment_id = ha.assessment_id
left join dict.habitat_system_mapping m on m.habitat_code=ha.code
LEFT JOIN detail.threats AS th ON a.assessment_id = th.assessment_id
left join dict.threats__dict th_d on th.code = th_d.code
