
{{
    config (
        materialized = 'table'
    )
}}

with org_data as (
    
     select 
    
    0 as APPROXIMATECREATIONDATETIME,
    org_data.ITEMID,
    org_data.S3KEY,
    org_data.ATTACHMENTID,
    org_data.TYPE,
    org_data.ANNOTATION

from "NIRSTREAMTEST01"."DBT_NIR"."STREAM_NIR_ABUCHNICKTHAI_AIOLA_INSPECTION_ATTACHMENTS_TABLE" as org_data

)

select * from org_data