{{ config(schema = 'RAW',materialized= 'view') }}

select Anio,Presupuesto, Costo FROM {{ source('Services','Presupuesto_anual_COSEVI')}}