{{
    config (
        materialized = 'incremental',
        unique_key='ITEMID',
        incremental_strategy='merge',
        pre_hook = "SELECT * FROM {{ ref('base_dim_prehook_delete_modify') }} "
    )
}}


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

        WHERE APPROXIMATECREATIONDATETIME > (SELECT * FROM attachements_maxdatetime)
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