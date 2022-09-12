{{
    config (
        materialized = 'incremental'
    )
}}


select 
    org_data.ITEMID,
    org_data.S3KEY,
    org_data.ATTACHMENTID,
    org_data.type,
    org_data.ANNOTATION

 from {{ ref('stg_nir_buchnik_stream_attachments') }} as org_data