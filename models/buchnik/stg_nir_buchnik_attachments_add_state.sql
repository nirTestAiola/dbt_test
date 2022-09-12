{{
    config (
        materialized = 'table'
    )
}}

SELECT 
    org_table.APPROXIMATECREATIONDATETIME,
    org_table.ITEMID,
    org_table.S3KEY,
    org_table.ATTACHMENTID,
    org_table.TYPE,
    org_table.ANNOTATION,
    null as STATE

 FROM {{ ref('stg_nir_buchnik_attachments') }} as org_table