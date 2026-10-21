# Ariflow


## Data quality checks


Structural checks are integrated in the tasks because if there is no data or the structure is invalid, we won't be able to run any other checks.
Critical and non-critical checks are implemented in a separate class

statuses:
- ok
- failed
- unsupported
- error (during execution)

Optimization:
- data types
- indices
- explain analyze



Built an Airflow pipeline that extracts IUCN assessment data, validates completeness and uniqueness, and loads 5,526 latest assessments into ClickHouse marts powering a Superset dashboard.

Number of API records extracted.

Number of assessments loaded into the detail layer.

Number of marts and dashboard charts.

DAG schedule and runtime.

Data-quality checks implemented.

Whether reruns are idempotent.

For example:

Built an Airflow pipeline that extracts IUCN assessment data, validates completeness and uniqueness, and loads 5,526 latest assessments into ClickHouse marts powering a Superset dashboard.

---
IUCN Red List is updated only a few times a year ([source](https://www.iucnredlist.org/assessment/updates)), so there is no need (and frankly it is wasteful) to check for new assessments every day, especially when there is no way of limiting API queries with granularity smaller than year. That is why all the DAGs are configured to be ran manually.
