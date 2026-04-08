{{ config(materialized='view') }}

SELECT
    LOCATION_ID,
    LOWER(LOCATION_NAME)                    AS location_name,
    LOWER(CITY)                             AS city,
    UPPER(COUNTRY)                          AS country,
    LATITUDE,
    LONGITUDE,
    LOWER(PARAMETER)                        AS parameter,
    VALUE                                   AS reading_value,
    UNIT,
    WHO_THRESHOLD,
    EXCEEDS_WHO,
    MEASURED_AT                             AS measured_at,
    DATE_TRUNC('hour', MEASURED_AT)         AS measured_hour,
    DATE(MEASURED_AT)                       AS measured_date,
    INGESTED_AT,
    CASE UPPER(COUNTRY)
        WHEN 'NG' THEN 'Nigeria'
        WHEN 'GH' THEN 'Ghana'
        WHEN 'KE' THEN 'Kenya'
        WHEN 'ZA' THEN 'South Africa'
        WHEN 'ET' THEN 'Ethiopia'
        ELSE COUNTRY
    END                                     AS country_name
FROM {{ source('raw', 'AIR_QUALITY_READINGS') }}
WHERE VALUE IS NOT NULL
  AND VALUE >= 0
  AND MEASURED_AT IS NOT NULL
