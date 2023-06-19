{{ config(schema = 'RAW',materialized= 'view') }}

select Anio,Presupuesto FROM {{ source('Services','Presupuesto_anual_COSEVI')}}