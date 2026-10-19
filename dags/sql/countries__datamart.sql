-- replace
CREATE TABLE IF NOT EXISTS datamart.countries__datamart
(
    `assessment_id` UInt64,
    `country`       LowCardinality(String) default 'empty',
    `code`          Nullable(String),
    `CREATE_DTTM`   DateTime default now()
)
ORDER BY (assessment_id)
;

TRUNCATE TABLE datamart.countries__datamart;

--EXPLAIN ANALYZE
INSERT INTO datamart.countries__datamart (* EXCEPT(`CREATE_DTTM`))
select 
    l.assessment_id as assessment_id, 
    l.country, 
    l.code 
from detail.assessments a 
left join detail.locations l using assessment_id
WHERE latest = 1;
