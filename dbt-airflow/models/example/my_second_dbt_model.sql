
-- Use the `ref` function to select from other models

{{ config(
    unique_key='id',
    incremental_strategy='append'
) }}

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
