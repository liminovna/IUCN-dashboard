-- replace
CREATE TABLE IF NOT EXISTS datamart.countries__datamart
(
    `assessment_id` UInt64                                  COMMENT 'ID of assessment',
    `country`       LowCardinality(String) default 'empty'  COMMENT 'Country where the species has ever been recorded in',                                
    `code`          Nullable(String)                        COMMENT 'Code of the country where the species has ever been recorded in',            
    `CREATE_DTTM`   DateTime default now()                  COMMENT 'Timestamp when the record was added to the table'
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
