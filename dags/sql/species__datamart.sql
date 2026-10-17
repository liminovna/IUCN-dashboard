-- replace
CREATE TABLE IF NOT EXISTS datamart.species__datamart
(
    `scientific_name`              String,
    `kingdom_name`                 Nullable(String),
    `phylum_name`                  Nullable(String),
    `class_name`                   Nullable(String),
    `order_name`                   Nullable(String),
    `family_name`                  Nullable(String),
    `genus_name`                   Nullable(String),
    `species_name`                 Nullable(String),
    `assessment_id`                UInt64,
    `assessment_date`              Nullable(DateTime),
    `year_published`               UInt16,
    `possibly_extinct`             Nullable(Bool),
    `possibly_extinct_in_the_wild` Nullable(Bool),
    `url`                          Nullable(String),
    `population_trend`             Nullable(String),
    `population_size`              Nullable(String),
    `red_list_category`            Nullable(String),
    `red_list_category_code`       Nullable(String),
    `scopes`                       Array(String),
    `countries`                    Array(String),
    `systems`                      Array(String),
    `habitats`                     Array(String),
    `threats`                      Array(String),
    `stresses`                     Array(String),
    `conservation_actions`         Array(String),
    `CREATE_DTTM`                  DateTime default now()
);

TRUNCATE TABLE datamart.species__datamart;

INSERT INTO datamart.species__datamart (* EXCEPT(`CREATE_DTTM`))
WITH latest_assessments AS
(
    SELECT
        assessment_id,
        sis_taxon_id,
        scientific_name,
        assessment_date,
        year_published,
        possibly_extinct,
        possibly_extinct_in_the_wild,
        url,
        population_trend,
        population_size,
        red_list_category,
        red_list_category_code
    FROM detail.assessments a
    WHERE latest = 1
    AND not exists (select 1 from datamart.overview__datamart d where a.assessment_id = d.assessment_id)
),
country_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(country) AS countries
    FROM detail.locations
    GROUP BY assessment_id
),
system_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(system) AS systems
    FROM detail.systems
    GROUP BY assessment_id
),
scope_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(scope) as scopes
    FROM detail.scopes
    GROUP BY assessment_id
),
habitat_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(habitat_lvl1) AS habitats
    FROM detail.habitats ha
    left join dict.habitat_system_mapping m on ha.code = m.habitat_code
    GROUP BY assessment_id
),
threat_arrays AS
(
    SELECT
        th.assessment_id,
        groupUniqArray(th_d.threat_lvl1) AS threats
    FROM detail.threats th
    inner join dict.threats__dict th_d on th_d.code = th.code
    GROUP BY th.assessment_id
),
stress_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(stress) AS stresses
    FROM detail.stresses
    GROUP BY assessment_id
),
action_arrays AS
(
    SELECT
        assessment_id,
        groupUniqArray(description) AS conservation_actions
    FROM detail.conservation_actions
    WHERE not (code ilike '5_4%' or code ilike '5_1%')
    GROUP BY assessment_id
)
SELECT
    t.scientific_name as scientific_name,
    t.kingdom_name,
    t.phylum_name,
    t.class_name,
    t.order_name,
    t.family_name,
    t.genus_name,
    t.species_name,

    a.assessment_id as assessment_id,
    a.assessment_date,
    a.year_published,
    a.possibly_extinct,
    a.possibly_extinct_in_the_wild,
    a.url,
    a.population_trend,
    a.population_size,
    a.red_list_category,
    a.red_list_category_code,
    sc.scopes AS scopes,

    coalesce(ca.countries, []) AS countries,
    coalesce(sa.systems, []) AS systems,
    coalesce(ha.habitats, []) AS habitats,
    coalesce(ta.threats, []) AS threats,
    coalesce(st.stresses, []) AS stresses,
    coalesce(aa.conservation_actions, []) AS conservation_actions

FROM latest_assessments AS a
LEFT JOIN detail.taxonomy AS t
    ON a.sis_taxon_id = t.sis_taxon_id
LEFT JOIN country_arrays AS ca
    ON a.assessment_id = ca.assessment_id
LEFT JOIN system_arrays AS sa
    ON a.assessment_id = sa.assessment_id
LEFT JOIN habitat_arrays AS ha
    ON a.assessment_id = ha.assessment_id
LEFT JOIN threat_arrays AS ta
    ON a.assessment_id = ta.assessment_id
LEFT JOIN stress_arrays AS st
    ON a.assessment_id = st.assessment_id
LEFT JOIN scope_arrays AS sc
    ON a.assessment_id = sc.assessment_id
LEFT JOIN action_arrays AS aa
    ON a.assessment_id = aa.assessment_id
ORDER BY
    a.scientific_name,
    a.year_published DESC;