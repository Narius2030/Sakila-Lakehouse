
/*
    {{ config(
        unique_key='id',
        incremental_srategy='merge'
    ) }}
*/


select * from {{ source('delta-streamify', 'payment') }}
