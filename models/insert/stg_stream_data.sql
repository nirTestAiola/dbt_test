with stream_data as (

select 

    stream_data.eventname,
    stream_data.APPROXIMATECREATIONDATETIME,
    stream_data.Id,
    stream_data.authors,
    stream_data.BICYCLETYPE,
    stream_data.brand,
    stream_data.color,
    stream_data.description,
    stream_data.dimensions,
    stream_data.inpublication,
    stream_data.isbn,
    stream_data.pagecount,
    stream_data.price,
    stream_data.PRODUCTCATEGORY,
    stream_data.title

from "NIRSTREAMTEST01"."TRY01"."STREAM_DATA_01" as stream_data
)

select * from stream_data