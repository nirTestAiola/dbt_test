{{
    config (
        materialized = 'table'
    )
}}

with removed_modified as (
SELECT * 

FROM {{ ref('stg_origin_data') }} AS org_data

WHERE

org_data.ID not in (

    SELECT removed.ID FROM {{ ref('stg_stream_data_update') }} AS removed
)
)

SELECT *
FROM removed_modified

UNION ALL 

select * from {{ ref('stg_stream_data_update') }} as org_data