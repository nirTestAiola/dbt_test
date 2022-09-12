{{
    config (
        materialized = 'table'
    )
}}

with max_event_time as (
    SELECT ITEMID, MAX(APPROXIMATECREATIONDATETIME) as max_APPROXIMATECREATIONDATETIME FROM {{ ref('stg_nir_buchnik_stream_attachments') }}
    GROUP BY ITEMID
)

select 

    stg_stream.EVENTID,
    stg_stream.EVENTNAME,
    stg_stream.APPROXIMATECREATIONDATETIME,
    stg_stream.SIZEBYTES,
    stg_stream.ITEMID,
    stg_stream.S3KEY,
    stg_stream.ATTACHMENTID,
    stg_stream.TYPE,
    stg_stream.ANNOTATION,
    stg_stream.state

 from {{ ref('stg_nir_buchnik_stream_attachments') }} as stg_stream
right join max_event_time 
on stg_stream.ITEMID = max_event_time.ITEMID and stg_stream.APPROXIMATECREATIONDATETIME = max_event_time.max_APPROXIMATECREATIONDATETIME
