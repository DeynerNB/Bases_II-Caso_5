{{ config(schema = 'RAW',materialized= 'view') }}

select servicio,precioCita FROM {{ source('Services','ServiciosCOSEVI')}}