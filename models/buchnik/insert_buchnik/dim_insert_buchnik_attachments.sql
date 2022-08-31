{{
    config (
        materialized = 'table'
    )
}}


select * from {{ ref('stg_nir_buchnik_attachments') }} as org_data

UNION ALL 

select * from {{ ref('stg_stream_nir_buchnik_attachments') }} as org_data
