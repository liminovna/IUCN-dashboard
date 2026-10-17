-- replace
CREATE TABLE IF NOT EXISTS datamart.countries__datamart
(
    `assessment_id` UInt64,
    `country`       String,
    `code`          Nullable(String),
    `CREATE_DTTM`   DateTime default now()
)
;

TRUNCATE TABLE datamart.countries__datamart;

--EXPLAIN ANALYZE
INSERT INTO datamart.countries__datamart (* EXCEPT(`CREATE_DTTM`))
WITH latest_assessments AS
(
    SELECT
        assessment_id
    FROM detail.assessments a
    WHERE latest = 1
    AND not exists (select 1 from datamart.overview__datamart d where a.assessment_id = d.assessment_id)
)
select 
    l.assessment_id as assessment_id, 
    l.country, 
    l.code 
from detail.locations l
where l.assessment_id in (latest_assessments);

