
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(
    unique_key='id'
) }}


with source_data as (
    select 1 as id, 'narius' as user_name
    union all
    select 2 as id, 'andy' as user_name
)

select * from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
