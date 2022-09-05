{{
    config (
        materialized = 'table'
    )
}}

SELECT max(APPROXIMATECREATIONDATETIME) as max_APPROXIMATECREATIONDATETIME FROM {{ 'stg_nir_buchnik_stream_attachments' }}