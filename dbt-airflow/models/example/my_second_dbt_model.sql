/*
    {{ config(
        unique_key='id',
        incremental_srategy='merge'
    ) }}
*/


select *
from {{ ref('my_first_dbt_model') }}
where id < 100
