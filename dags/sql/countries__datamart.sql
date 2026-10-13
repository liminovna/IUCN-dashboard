-- increment
CREATE TABLE IF NOT EXISTS datamart.countries__datamart
(
    `assessment_id` UInt64,
    `country`       String,
    `code`          Nullable(String),
    `CREATE_DTTM`   DateTime default now()
)
;

--EXPLAIN ANALYZE
INSERT INTO datamart.countries__datamart (* EXCEPT(`CREATE_DTTM`))
WITH latest_assessments AS
(
    SELECT
        assessment_id
    FROM detail.assessments
    WHERE latest = 1
    AND `CREATE_DTTM` >= (select max(`CREATE_DTTM`) from datamart.countries__datamart)
)
select 
    l.assessment_id as assessment_id, 
    l.country, 
    l.code 
from detail.locations l
where l.assessment_id in (latest_assessments);

