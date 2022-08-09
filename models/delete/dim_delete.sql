{{
    config (
        materialized = 'table'
    )
}}


SELECT * 

FROM {{ ref('stg_origin_data') }} AS org_data

WHERE

org_data.ID not in (

    SELECT removed.ID FROM {{ ref('stg_stream_data_delete') }} AS removed
)
