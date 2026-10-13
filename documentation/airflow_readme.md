# Russia’s Biodiversity: IUCN Red List Overview

The following project explores IUCN Red List assessments published between 2020 and 2026 for species recorded in the Russian Federation, focusing on conservation status, population trends, threats, and recommended conservation actions. Here, we try to answers the folowing questions:

1. What is the current conservation status of species recorded in the Russian Federation?
2. How have conservation statuses and population trends changed over time?
3. What are the most common threats affecting species with declining populations, and which conservation actions can help most?

## Quick note on IUCN
The International Union for Conservation of Nature’s (IUCN) Red List of Threatened Species is the world’s most comprehensive information source on the global conservation status of animal, fungi and plant species. 

<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
  <div>
    <p>It divides species into nine categories:</p> 
    <ul>
    <li>Extinct (EX) – There are no known living individuals</li>
    <li>Extinct in the wild (EW) – Known only to survive in captivity, or as a naturalized population outside its historic range</li>
    <li>Critically Endangered (CR) – Highest risk of extinction in the wild</li>
    <li>Endangered (EN) – Higher risk of extinction in the wild</li>
    <li>Vulnerable (VU) – High risk of extinction in the wild</li>
    <li>Near Threatened (NT) – Likely to become endangered in the near future</li>
    <li>Conservation Dependent (CD) – Low risk; is conserved to prevent being near threatened, certain events may lead it to being a higher risk level </li>
    <li>Least concern (LC) – Very low risk; does not qualify for a higher risk category and not likely to be threatened in the near future. </li>    
    <li>Widespread and abundant taxa are included in this category.</li>
    <li>Data deficient (DD) – Not enough data to make an assessment of its risk of extinction</li>
    <li>Not evaluated (NE) – Has not yet been evaluated against the criteria.</li>
    </ul>
  </div>
  <div style="text-align: center;">
    <p><img src="assets/asian_black_bear.png" alt="Screenshot from a Wikipedia article on an Asian black bear" width="200"/></p>
    <p><i><a href"https://en.wikipedia.org/wiki/Asian_black_bear">Example</a> of a species given a conservation status by IUCN</i></p>
  </div>
</div>

## Data
On September 29th 2026, the data encompassess 6,230 assessments published between year 2020 and 2026 on species recorded in Russia. The data was scraped from [IUCN Red List API v4](https://api.iucnredlist.org/).

## Skills and tools
This os a pet project developed to showcase the following skills and tools:
- Business Intelligence and Reporting: Apache Superset
- Analytics and Insight delivery
- SQL: Clickhouse
- Data engineering and Data quality monitorng: Apache Airflow

The project also involved docker, git, html and css.

## Insights
**Q1. What is the current conservation status of species recorded in the Russian Federation?**

Between 2020 and 2026, the IUCN published assessments for 5,526 species recorded in Russia. Among the latest assessments, Least Concern is by far the most common Red List category, accounting for 75% of assessed species. Population trends show a similar pattern: 39% of assessments report a stable population trend. Taken together, 38% of the latest assessments classify a species as Least Concern with a stable population trend, making this the largest combined group.

However, because assessment coverage is heavily biased toward animals, it is useful to consider the three kingdoms separately. The dominance of Least Concern persists across all groups and is especially pronounced among plants (90%) and fungi (80%). A stable population trend is also the most common trend within each kingdom.

Threatened species with declining populations represent 8% of assessed fungi, 7% of animals, and 2% of plants. However, it should be taken into account that the number of assessed fungi and plants is fairly low -- 213 and 320 respectively. 

In addition, 10% of species are classified as Data Deficient or Not Applicable, highlighting the limits of current assessment coverage.

**Q2. How have conservation statuses and population trends changed over time?**

The number of assessments carried out (not published!) each year has been uneven since 2018. The largest peaks occurred in 2020 (approximately 1,150 assessments) and 2023–2024 (approximately 3,740 assessments), while other years recorded fewer than 500 assessments.

In years with a high volume of assessments, the proportion of assessments resulting in a threatened category or a declining population trend tends to be lower. Conversely, the highest proportions of threatened statuses and declining trends occur in years with relatively few assessments, which suggests an inverse relationship between assessment volume and the share of “unfavourable” outcomes.

This pattern should be interpreted cautiously, since it may reflect changes in the mix of species assessed, rather than an actual improvement or deterioration in conservation status.

**Q3. What are the most common threats affecting species with declining populations, and which conservation actions can help most?**

Among threatened animal species, the five most frequently recorded threats are agriculture and aquaculture, biological resource use, climate change and severe weather, pollution, and natural system modifications. Most of these threats are closely linked to human activity, unlike threats such as geological events or problematic species, genes, and diseases.

For conservation actions, habitat protection and awareness-raising are the most commonly recommended measures.

Fungi show a similar pattern: human activity is a major source of pressure, and habitat protection and restoration are the most frequently recommended responses.

Among the eight threatened plant species, threats also include problematic species, genes, and diseases. Recommended actions include habitat protection and restoration, harvest management, and control of problematic species.

**Conclusion**

IUCN assessment coverage for species recorded in Russia is dominated by animals, with comparatively limited coverage of plants and fungi. Across all three kingdoms, most assessed species are classified as Least Concern and have stable population trends, suggesting a relatively favourable conservation status among assessed species.

However, the annual distribution of assessments is highly uneven, with peaks in 2020 and 2023–2024. These peaks coincide with lower proportions of threatened species, which may reflect differences in the species assessed rather than genuine changes in extinction risk.

Among threatened species, human-related threats are the most common, and habitat protection appears to be the most broadly recommended conservation action. While it may not eliminate all risks, habitat protection is likely to be one of the most effective measures to help prevent further decline.

## Screenshots

## Project structure

## Limitations
- The data only encompasses 7 years of research due to resource limitations
- IUCN coverage is not random, and we cannot judge about the general conservation status by assessments
- A more extensive analysis requires more data on Green List statuses (https://www.iucnredlist.org/about/green-status-species)

## Backlog
- add full-text analysis of threats, trade and use, conservation actions
- add info on transitions between statuses (requires more data)
- add more detailed geographical ranges
- expand time range of assessments 

# Sources
IUCN 2026. IUCN Red List of Threatened Species. Version 2026-1 <www.iucnredlist.org> 
