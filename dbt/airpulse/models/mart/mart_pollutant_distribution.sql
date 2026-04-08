{{ config(
    materialized='table',
    cluster_by=['country', 'parameter']
) }}

SELECT
    country,
    country_name,
    parameter,
    COUNT(*)                                        AS reading_count,
    ROUND(AVG(reading_value), 2)                    AS avg_value,
    ROUND(MIN(reading_value), 2)                    AS min_value,
    ROUND(MAX(reading_value), 2)                    AS max_value,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
          (ORDER BY reading_value), 2)              AS p95_value,
    MAX(who_threshold)                              AS who_threshold,
    SUM(CASE WHEN exceeds_who THEN 1 ELSE 0 END)    AS breach_count,
    ROUND(
        100.0 * SUM(CASE WHEN exceeds_who THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                               AS breach_pct,
    MAX(measured_at)                                AS last_reading_at
FROM {{ ref('stg_air_quality') }}
GROUP BY country, country_name, parameter
ORDER BY country, parameter
