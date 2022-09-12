{{
    config (
        materialized = 'incremental',
        unique_key='ITEMID',
        incremental_strategy='merge',
        pre_hook = "DELETE FROM {{ ref('stg_nir_buchnik_attachments') }} as stg_stream WHERE stg_stream.ITEMID in (select distinct ITEMID from {{ ref('stg_stream_latest_event') }} as org_data WHERE EVENTNAME = 'REMOVE' AND org_data.APPROXIMATECREATIONDATETIME > ( SELECT max(org_data.APPROXIMATECREATIONDATETIME) from {{ ref('stg_nir_buchnik_attachments') }} as org_data) )"
    )
}}

-- depends_on: {{ ref('attachements_maxdatetime') }}

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

        WHERE APPROXIMATECREATIONDATETIME > (SELECT * FROM {{ ref('attachements_maxdatetime')}})
        AND EVENTNAME != 'REMOVE'

    {% endif %}
    

),

updates AS (

    SELECT
        ITEMID,
        s3key,
        ATTACHMENTID,
        type,
        annotation

    FROM using_clause

    {% if is_incremental() %}

        WHERE ITEMID IN (SELECT ITEMID FROM {{ this }})
        AND EVENTNAME = 'MODIFY'

    {% endif %}

),

inserts AS (

    SELECT
        ITEMID,
        s3key,
        ATTACHMENTID,
        type,
        annotation

    FROM using_clause

    WHERE ITEMID NOT IN (SELECT ITEMID FROM updates)
    AND EVENTNAME = 'INSERT'

)

SELECT * FROM updates 
UNION
SELECT * FROM inserts
