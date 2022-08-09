with stream_data_insert as (

select 

    stream_data_insert.APPROXIMATECREATIONDATETIME,
    stream_data_insert.Id,
    stream_data_insert.authors,
    stream_data_insert.BICYCLETYPE,
    stream_data_insert.brand,
    stream_data_insert.color,
    stream_data_insert.description,
    stream_data_insert.dimensions,
    stream_data_insert.inpublication,
    stream_data_insert.isbn,
    stream_data_insert.pagecount,
    stream_data_insert.price,
    stream_data_insert.PRODUCTCATEGORY,
    stream_data_insert.title

from {{ ref('stg_stream_data') }} as stream_data_insert
where stream_data_insert.eventname = 'MODIFY'
)

select * from stream_data_insert