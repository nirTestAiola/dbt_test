{{
    config (
        materialized = 'table'
    )
}}

with rows_to_insert_first (
    SELECT * FROM {{ ref('stg_stream_latest_event') }} as streamed_data
    WHERE streamed_data.APPROXIMATECREATIONDATETIME > ( SELECT max(org_data.APPROXIMATECREATIONDATETIME) from {{ ref('stg_nir_buchnik_attachments') }} as org_data) 
    AND EVENTNAME in ('MODIFY','INSERT')
)
,
rows_to_insert_second (
    stg_stream.EVENTID,
    stg_stream.EVENTNAME,
    stg_stream.APPROXIMATECREATIONDATETIME,
    stg_stream.SIZEBYTES,
    stg_stream.ITEMID,
    stg_stream.S3KEY,
    stg_stream.ATTACHMENTID,
    stg_stream.TYPE,
    stg_stream.ANNOTATION,

    CASE stg_stream.STATE
        WHEN stg_stream.EVENTNAME = 'MODIFY' THEN 'intermediate'
)

SELECT 
    APPROXIMATECREATIONDATETIME,
    SIZEBYTES,
    ITEMID,
    S3KEY,
    ATTACHMENTID,
    TYPE,
    ANNOTATION,
    STATE

 rows_to_insert_second  
