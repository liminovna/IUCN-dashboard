-- replace
CREATE TABLE IF NOT EXISTS datamart.overview__datamart
(
    `assessment_id`           UInt64,
    `assessment_date`         DateTime,
    `kingdom_name`            LowCardinality(String) default 'empty',
    `scientific_name`         String default 'empty',
    `year_published`          UInt16,
    `population_trend`        LowCardinality(String) default 'empty',
    `red_list_category`       LowCardinality(String) default 'empty',
    `red_list_category_code`  LowCardinality(String) default 'empty',
    `category_sorted`         LowCardinality(String) default 'empty',
    `threat`                  LowCardinality(String) default 'empty',
    `system`                  LowCardinality(String) default 'empty',
    `use_and_trade`           LowCardinality(String) default 'empty',
    `conservation_action`     LowCardinality(String) default 'empty',
    `CREATE_DTTM`             DateTime default now()
)
ORDER BY (system, population_trend, year_published, category_sorted);

TRUNCATE TABLE datamart.overview__datamart;

-- explain analyze
INSERT INTO datamart.overview__datamart (* EXCEPT(`CREATE_DTTM`))
SELECT distinct
    a.assessment_id as assessment_id,
    a.assessment_date,
    t.kingdom_name,
    a.scientific_name as scientific_name,
    a.year_published,
    a.population_trend,
    a.red_list_category,
    a.red_list_category_code,
    coalesce(rl.category_sorted, '0. Not Evaluated') as category_sorted,
    coalesce(th_d.threat_lvl1, 'Type Unknown/Unrecorded') as threat,
    sy.system,
    case when ut.use_and_trade = '' then null else ut.use_and_trade end as use_and_trade,
    ca.description as conservation_action
FROM dict.red_list_categories_mapping rl
full join detail.assessments AS a on rl.code = a.red_list_category_code and a.latest = 1
left join detail.taxonomy t using sis_taxon_id
LEFT JOIN detail.systems AS sy ON a.assessment_id = sy.assessment_id
-- LEFT JOIN detail.habitat AS ha ON a.assessment_id = ha.assessment_id
LEFT JOIN detail.use_and_trade AS ut ON a.assessment_id = ut.assessment_id
LEFT JOIN detail.threats AS th ON a.assessment_id = th.assessment_id
left join dict.threats__dict th_d on th.code = th_d.code
left join detail.conservation_actions ca on a.assessment_id = ca.assessment_id
WHERE version = '3.1';