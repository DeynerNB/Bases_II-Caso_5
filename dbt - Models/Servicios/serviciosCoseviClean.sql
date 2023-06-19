-- Modelo 5: NuevoModelo
{{ config(schema = 'Clean', materialized = 'view', depends_on=[
    {'model': 'paginasRAW', 'strategy': 'skip'},
    {'model': 'presupuestoServicios', 'strategy': 'skip'}
]) }}

select
  row_number() OVER (ORDER BY pServ.servicio) AS id,
  pServ.servicio as Servicio,
  pServ.precioCita as Precio,
  pServ.anio as Anio,
  pServ.presupuestoServicio as Costo_Operativo,
  case
    when pServ.servicio in ('Cita para prueba teorica de manejo', 'Cita para prueba practica de manejo', 'Cita para permiso de conducir') then (SELECT link FROM {{ ref('paginasRAW') }} WHERE id = 12493)
    else (SELECT link FROM {{ ref('paginasRAW') }} WHERE id = 251)
  end as Pagina
from
  {{ ref('presupuestoServicios') }} as pServ
cross join
  {{ ref('paginasRAW') }} as paginas
