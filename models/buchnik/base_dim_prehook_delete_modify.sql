{{
    config (
        materialized = 'table'
        )
}}

SELECT * FROM {{ ref('stg_nir_buchnik_attachments') }} as stg_stream
WHERE stg_stream.ITEMID not in (select distinct ITEMID from {{ ref('stg_stream_latest_event') }} as org_data
WHERE EVENTNAME = 'REMOVE'
AND org_data.APPROXIMATECREATIONDATETIME > ( SELECT max(org_data.APPROXIMATECREATIONDATETIME) from {{ ref('stg_nir_buchnik_attachments') }} as org_data) 
)
/*
        pre_hook = "SELECT * FROM {{ ref('stg_nir_buchnik_attachments') }} as stg_stream WHERE stg_stream.ITEMID not in (select distinct ITEMID from {{ ref('stg_stream_latest_event') }} as org_data WHERE EVENTNAME = 'REMOVE' AND org_data.APPROXIMATECREATIONDATETIME > ( SELECT max(org_data.APPROXIMATECREATIONDATETIME) from {{ ref('stg_nir_buchnik_attachments') }} as org_data) )"
*/