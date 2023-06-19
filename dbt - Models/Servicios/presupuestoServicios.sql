-- Modelo 3: presupuestoServiciosEquitativos
{{ config(schema = 'RAW', materialized = 'view', depends_on=[
    {'model': 'serviciosCoseviRAW','strategy':'skip'},
    {'model': 'paginasRAW','strategy':'skip'},
]) }}

select
  serv.servicio,
  serv.precioCita,
  pres.Anio,
  pres.Presupuesto / count(*) over (partition by pres.Anio) as presupuestoServicio
from
  [RAW].serviciosCoseviRAW as serv
cross join
  [RAW].presupuestoRAW as pres
group by
  serv.servicio,
  serv.precioCita,
  pres.Anio,
  pres.Presupuesto