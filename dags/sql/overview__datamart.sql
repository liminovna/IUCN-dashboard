-- replace
CREATE TABLE IF NOT EXISTS datamart.overview__datamart
(
    `assessment_id`           UInt64,
    `assessment_date`         Nullable(DateTime),
    `kingdom_name`            Nullable(String),
    `scientific_name`         String,
    `year_published`          UInt16,
    `population_trend`        Nullable(String),
    `red_list_category`       Nullable(String),
    `red_list_category_code`  Nullable(String),
    `category_sorted`         Nullable(String),
    `threat`                  Nullable(String),
    `system`                  String,
    `use_and_trade`           Nullable(String),
    `conservation_action`     Nullable(String),
    `CREATE_DTTM`             DateTime default now()
);

TRUNCATE TABLE datamart.overview__datamart;

-- explain analyze
INSERT INTO datamart.overview__datamart (* EXCEPT(`CREATE_DTTM`))
WITH red_list_code as (
    select code, category_sorted 
    from dict.red_list_categories_mapping
    where category_sorted is not null 
),
latest_assessments AS
(
    SELECT
        a.assessment_id,
        a.sis_taxon_id,
        t.kingdom_name,
        a.scientific_name,
        a.assessment_date,
        a.year_published,
        a.population_trend,
        a.population_size,
        a.red_list_category,
        a.red_list_category_code
    FROM detail.assessments a
    left join detail.taxonomy t using sis_taxon_id
    WHERE a.latest = 1
)
SELECT distinct
    a.assessment_id as assessment_id,
    a.assessment_date,
    a.kingdom_name,
    a.scientific_name,
    a.year_published,
    a.population_trend,
    a.red_list_category,
    a.red_list_category_code,
    rl.category_sorted,
    coalesce(th_d.threat_lvl1, 'Type Unknown/Unrecorded') as threat,
    sy.system,
    case when ut.use_and_trade = '' then null else ut.use_and_trade end as use_and_trade,
    ca.description as conservation_action
FROM red_list_code rl
left join latest_assessments AS a on rl.code = a.red_list_category_code
LEFT JOIN detail.systems AS sy ON a.assessment_id = sy.assessment_id
-- LEFT JOIN detail.habitat AS ha ON a.assessment_id = ha.assessment_id
LEFT JOIN detail.use_and_trade AS ut ON a.assessment_id = ut.assessment_id
LEFT JOIN detail.threats AS th ON a.assessment_id = th.assessment_id
left join dict.threats__dict th_d on th.code = th_d.code
left join detail.conservation_actions ca on a.assessment_id = ca.assessment_id;