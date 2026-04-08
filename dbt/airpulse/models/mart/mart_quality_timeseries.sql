{{ config(
    materialized='table',
    cluster_by=['country', 'parameter', 'measured_hour']
) }}

SELECT
    country,
    country_name,
    parameter,
    measured_hour,
    measured_date,
    ROUND(AVG(reading_value), 2)                    AS avg_value,
    ROUND(MAX(reading_value), 2)                    AS max_value,
    MAX(who_threshold)                              AS who_threshold,
    COUNT(*)                                        AS reading_count,
    SUM(CASE WHEN exceeds_who THEN 1 ELSE 0 END)    AS breach_count
FROM {{ ref('stg_air_quality') }}
GROUP BY country, country_name, parameter, measured_hour, measured_date
ORDER BY country, parameter, measured_hour
