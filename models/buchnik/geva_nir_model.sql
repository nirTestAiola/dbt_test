{{
    config (
        materialized = 'table'
    )
}}

with rows_to_insert (
    SELECT * FROM {{ ref('stg_stream_latest_event') }} as streamed_data
    WHERE streamed_data.APPROXIMATECREATIONDATETIME > ( SELECT max(org_data.APPROXIMATECREATIONDATETIME) from {{ ref('stg_nir_buchnik_attachments') }} as org_data) 
    AND EVENTNAME in ('MODIFY','INSERT')
)

INSERT INTO {{ ref('stg_nir_buchnik_attachments') }} as org_data 
SELECT 
