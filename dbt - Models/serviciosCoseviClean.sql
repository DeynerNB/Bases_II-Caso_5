{{ config(schema = 'RAW', materialized = 'table') }}

select
  serv.servicio,
  serv.precioCita,
  pres.anio,
  pres.presupuesto
from
  {{ ref('Services', 'ServiciosCOSEVI') }} as serv,
  tu_tabla_de_presupuestos as pres
order by 
    pres.anio