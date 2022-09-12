{{
    config (
        materialized = 'table'
    )
}}

with deleted_keys as (
    SELECT ITEMID FROM {{ ref('stg_stream_latest_event') }} AS latest_events
    WHERE EVENTNAME = 'REMOVE'

)
UPDATE {{ ref('stg_nir_buchnik_attachments') }} AS org_data SET state = 'deleted'
WHERE org_data.ITEMID in deleted_keys