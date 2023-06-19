{{ config(schema = 'RAW',materialized= 'view') }}

select id,link from {{ source('Services','Pages')}}
where id= 251 or id=12493
