/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    Try changing "table" to "view" below
*/


{{ config(materialized='view',
contracts={
  "producer": {
    "finance-only": {
      "version": "0.1.0",
      "requirements": {
        "test_coverage": { "enabled": true },
        "freshness_coverage": { "enabled": true },
        "run_history": 5,
        "success_only": true,
        "max_upgrade_time": { "days": 10 }
      },
      "security": { "api_public_key": "asfawef3" }
    }
  }
}
) }}


with source_data as (

    select 1 as id
    union all
    select 2 as id

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
