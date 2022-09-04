{{
    config (
        materialized = 'incremental',
        unique_key='ITEMID',
        incremental_strategy='merge',
        schema="BUCHNIK",
        pre_hook = "SELECT * FROM {{ ref('base_dim_prehook_delete_modify') }} "
    )
}}
/*
SELECT 
    stream_data.ITEMID,
    stream_data.APPROXIMATECREATIONDATETIME,
    stream_data.s3key,
    stream_data.ATTACHMENTID,
    stream_data.type,
    stream_data.annotation

 FROM {{ ref('stg_stream_latest_event') }} as stream_data

 WHERE stream_data.APPROXIMATECREATIONDATETIME > ( SELECT max(org_data.APPROXIMATECREATIONDATETIME) from {{ ref('stg_nir_buchnik_attachments') }} as org_data)
*/

WITH

using_clause AS (

    SELECT
    ITEMID,
    APPROXIMATECREATIONDATETIME,
    s3key,
    ATTACHMENTID,
    type,
    annotation,
    EVENTNAME

    FROM {{ ref('stg_stream_latest_event') }}

    

    {% if is_incremental() %}

        WHERE APPROXIMATECREATIONDATETIME > (SELECT max(APPROXIMATECREATIONDATETIME) FROM {{ this }})
        AND EVENTNAME != 'REMOVE'

    {% endif %}
    

),

updates AS (

    SELECT
        ITEMID,
        APPROXIMATECREATIONDATETIME,
        s3key,
        ATTACHMENTID,
        type,
        annotation
        
    FROM using_clause

    {% if is_incremental() %}

        WHERE ITEMID IN (SELECT ride_id FROM {{ this }})
        AND EVENTNAME = 'MODIFY'

    {% endif %}

),

inserts AS (

    SELECT
        ITEMID,
        APPROXIMATECREATIONDATETIME,
        s3key,
        ATTACHMENTID,
        type,
        annotation

    FROM using_clause

    WHERE ITEMID NOT IN (SELECT ITEMID FROM updates)
    AND EVENTNAME = 'INSERT'

)

SELECT * FROM updates /* UNION inserts