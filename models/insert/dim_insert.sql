{{
    config (
        materialized = 'table'
    )
}}


select * from {{ ref('stg_origin_data') }} as org_data

UNION ALL 

select * from {{ ref('stg_stream_data_insert') }} as org_data
