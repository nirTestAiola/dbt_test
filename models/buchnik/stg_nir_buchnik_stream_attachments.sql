{{
    config (
        materialized = 'table'
    )
}}

with org_data as (
    
     select 
    
    org_data.EVENTID,
    org_data.EVENTNAME,
    org_data.APPROXIMATECREATIONDATETIME,
    org_data.SIZEBYTES,
    org_data.ITEMID,
    org_data.S3KEY,
    org_data.ATTACHMENTID,
    org_data.TYPE,
    org_data.ANNOTATION


from "NIRSTREAMTEST01"."BUCHNIK"."STREAM_NIR_ABUCHNICKTHAI_AIOLA_INSPECTION_ATTACHMENTS_TABLE" as org_data

)

select * from org_data