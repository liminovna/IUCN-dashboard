--replace
CREATE TABLE IF NOT EXISTS datamart.threats__datamart
(
    `assessment_id`           UInt64                                    COMMENT 'ID of assessment',
    `assessment_date`         Nullable(DateTime)                        COMMENT 'Date when the evaluation was finished by the assessor',
    `scientific_name`         String                                    COMMENT 'Scientific name of the species',
    `year_published`          UInt16                                    COMMENT 'Year when the evaluation was published',
    `population_trend`        LowCardinality(String) default 'empty'    COMMENT 'Estimated population trend (text)',
    `red_list_category`       LowCardinality(String) default 'empty'    COMMENT 'Estimated Red List category (text)',
    `red_list_category_code`  LowCardinality(String) default 'empty'    COMMENT 'Estimated Red List category (code)',
    `category_sorted`         LowCardinality(String) default 'empty'    COMMENT 'Estimated Red List category (text with added rank)',
    `timing`                  LowCardinality(String) default 'empty'    COMMENT 'Timing of the threat',                                
    `threat`                  String                                    COMMENT 'Threat',
    `system`                  String                                    COMMENT 'System of habitat',
    `habitat`                 LowCardinality(String) default 'empty'    COMMENT 'Habitat',                                
    `CREATE_DTTM`             DateTime default now()                    COMMENT 'Timestamp when the record was added to the table'
)
ORDER BY (system, population_trend, category_sorted, threat, habitat);

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
