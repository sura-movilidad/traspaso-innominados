# Concreces.ipynb

## Propósito

Carga la nómina mensual de recaudación del partner **Concreces** a la tabla histórica en SQL Server. Concreces entrega los datos en Excel; el notebook selecciona automáticamente el archivo más reciente, normaliza columnas, extrae el período desde el nombre del archivo y mapea los planes a sus códigos internos.

## Archivos de entrada

| Atributo          | Detalle                                                    |
|-------------------|------------------------------------------------------------|
| Carpeta           | `data/CCLA/CONCRECES/`                                     |
| Formato           | Excel (`.xlsx`)                                            |
| Hoja preferida    | `Base` / `base` (fallback: hoja índice 0)                  |
| Selección         | El archivo con la fecha de modificación más reciente       |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el Excel

| Columna en el Excel              | Campo en SQL Server      | Notas                                   |
|----------------------------------|--------------------------|-----------------------------------------|
| guia                             | `Guia`                   |                                         |
| nro_operacion / n_operacion      | `Operacion`              |                                         |
| poliza                           | `Poliza`                 |                                         |
| serie_r_u_t / rut_afiliado / rut | `RUT`                    |                                         |
| digito_r_u_t / dv_afiliado / dv  | `DV`                     |                                         |
| fecha_nacimiento                 | `Fecha_Nacimiento`       | Convertida a `date`                     |
| fecha_suscripcion                | `Fecha_Suscripcion`      | Convertida a `date`                     |
| plan / plan_tecnico              | `Plan_Tecnico`           | Mapeado desde nombre de plan (ver abajo)|
| prima_ces / prima                | `Prima_Ces`              |                                         |
| prima_bruta / prima_bruta_ppi    | `Prima_Bruta_PPI`        |                                         |
| monto_asegurado                  | `Monto_Asegurado`        |                                         |
| *(generado)*                     | `MES_RECAUDACION`        | Extraído del nombre del archivo (YYYYMM)|
| *(generado)*                     | `NOMBRE_ARCHIVO`         |                                         |

## Mapeo Plan → Plan_Tecnico

Definido en `config/concreses/config_concreces.json`:

| Plan (texto)     | Plan_Tecnico |
|------------------|-------------|
| Plan 3 Cuotas    | 4780        |
| Plan 4 Cuotas    | 4781        |
| Plan 6 Cuotas    | 4782        |
| Plan 8 Cuotas    | 5743        |
| Plan 12 Cuotas   | 5744        |
| Plan 24 Cuotas   | 6547        |
| Plan 36 Cuotas   | 7234        |
| Plan 48 Cuotas   | 8621        |

## Transformaciones principales

1. Selección automática del Excel más reciente con manejo de bloqueos OneDrive
2. Normalización de nombres de columna (snake_case, sin tildes)
3. Extracción de `MES_RECAUDACION` (YYYYMM) desde el nombre del archivo
4. Mapeo de nombre de plan a código interno `Plan_Tecnico`
5. Conversión de fechas (`Fecha_Nacimiento`, `Fecha_Suscripcion`) al tipo `date`
6. Casteo de montos a float, RUTs a Int64

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor                      |
|------------|----------------------------|
| Servidor   | SGF1034                    |
| Base       | Habitat                    |
| Schema     | dbo                        |
| Tabla      | `Recaudacion_Concreces`    |

## Configuración

Archivo: `config/concreses/config_concreces.json`

```json
{
    "tablas_remotas": { "tabla_principal": "Recaudacion_Concreces" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" },
    "mapa_planes":    { "Plan 3 Cuotas": 4780, "Plan 4 Cuotas": 4781, ... }
}
```
