
/* SELECT * FROM {{ ref('stg_nir_buchnik_attachments') }} as stg_stream
WHERE stg_stream.ITEMID not in (select ITEMID from {{ ref('stg_nir_buchnik_stream_attachments') }} as org_data
WHERE EVENTNAME != 'INSERT') */

select ITEMID,EVENTNAME from {{ ref('stg_stream_latest_event') }} as org_data
WHERE EVENTNAME = 'INSERT'