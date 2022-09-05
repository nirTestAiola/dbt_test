{{
    config (
        materialized = 'incremental'
    )
}}

/*
WITH fake_data AS (
    'fake_itemid' as ITEMID,
    'fake_s3key' as S3KEY,
    'fake_ATTACHMENTID' as ATTACHMENTID,
    'fake_type' as type,
    'fake_ANNOTATION' as ANNOTATION
)
*/

select 
    org_data.ITEMID,
    org_data.S3KEY,
    org_data.ATTACHMENTID,
    org_data.type,
    org_data.ANNOTATION

 from {{ ref('stg_nir_buchnik_stream_attachments') }} as org_data