{{
    config (
        materialized = 'table'
    )
}}

with deleted_keys as (
    SELECT ITEMID FROM {{ ref('stg_stream_latest_event') }} AS latest_events
    WHERE EVENTNAME = 'REMOVE'

)
,
modified_keys as (
    SELECT ITEMID FROM {{ ref('stg_stream_latest_event') }} AS latest_events
    WHERE EVENTNAME = 'MODIFY'

)

UPDATE {{ ref('stg_nir_buchnik_attachments_add_state') }} AS org_data 
SET state = CASE 
    WHEN org_data.ITEMID in deleted_keys THEN 'deleted'
    WHEN org_data.ITEMID in modified_keys THEN 'intermediate'
    END
