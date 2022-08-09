
with org_data as (
    
     select 
    
    0 as APPROXIMATECREATIONDATETIME,
    org_data.Id,
    org_data.authors,
    org_data.BICYCLETYPE,
    org_data.brand,
    org_data.color,
    org_data.description,
    org_data.dimensions,
    org_data.inpublication,
    org_data.isbn,
    org_data.pagecount,
    org_data.price,
    org_data.PRODUCTCATEGORY,
    org_data.title

from "NIRSTREAMTEST01"."TRY01"."ORIGINAL_DATA_01" as org_data

)

select * from org_data