# ServiHabit.ipynb

## Propósito

Carga la nómina mensual de recaudación del partner **ServiHabit** a la tabla histórica en SQL Server. ServiHabit entrega los datos en Excel; el notebook selecciona automáticamente el archivo más reciente, normaliza columnas y RUTs, extrae el período desde el nombre del archivo y carga los datos de forma idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                            |
|-------------------|--------------------------------------------------------------------|
| Carpeta           | `data/CCLA/SERVIHABIT/`                                            |
| Formato           | Excel (`.xlsx`)                                                    |
| Hoja preferida    | `Cesantia` / `cesantia` / `Recaudacion_ServiHabit` (fallback: hoja índice 0) |
| Selección         | El archivo con la fecha de modificación más reciente               |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el Excel

| Columna en el Excel                   | Campo en SQL Server   | Notas                                       |
|---------------------------------------|-----------------------|---------------------------------------------|
| guia                                  | `Guia`                |                                             |
| nro_operacion / n_operacion / folio   | `Operacion`           |                                             |
| svs / s_v_s                           | `Svs`                 |                                             |
| dueno / dueño / nombre_dueno          | `Dueno`               |                                             |
| serie_r_u_t / rut_afiliado / rut      | `Rut`                 | Mantenido como string original              |
| *(normalizado desde Rut)*             | `rut2`                | RUT numérico limpio (Int64)                 |
| *(normalizado desde Rut)*             | `dv2`                 | DV limpio                                   |
| nombre_cliente / cliente              | `Cliente`             |                                             |
| plazo                                 | `Plazo`               |                                             |
| poliza / nro_poliza                   | `Poliza`              |                                             |
| plan_tecnico / plan                   | `Plan_tecnico`        |                                             |
| cuota                                 | `Cuota`               |                                             |
| subsidio                              | `Subsidio`            |                                             |
| cuota_neta                            | `Cuota_Neta`          |                                             |
| prima_afecta_uf                       | `Prima_Afecta_uf`     |                                             |
| iva_afecta_uf                         | `Iva_Afecta_uf`       |                                             |
| total_prima_uf                        | `Total_Prima_uf`      |                                             |
| afecta_neta_clp                       | `Afecta_Neta_clp`     |                                             |
| iva_clp                               | `Iva_clp`             |                                             |
| total_prima_clp                       | `Total_Prima_CLP`     |                                             |
| *(generado)*                          | `MES_RECAUDACION`     | YYYYMM extraído del nombre del archivo      |
| *(generado)*                          | `NOMBRE_ARCHIVO`      |                                             |

## Transformaciones principales

1. Selección automática del Excel más reciente con manejo de bloqueos OneDrive
2. Normalización de nombres de columna (snake_case, sin tildes)
3. **Normalización de RUT:** la función `normaliza_rut_nueva` genera columnas `rut2` (numérico) y `dv2` (string) sin modificar el campo original `Rut`
4. Extracción de `MES_RECAUDACION` (YYYYMM) desde el nombre del archivo con fallback a `canonicalizar_planes()`
5. Casteo de importes a float (UF y CLP), RUTs a Int64
6. Columna `NOMBRE_ARCHIVO` se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor                       |
|------------|-----------------------------|
| Servidor   | SGF1034                     |
| Base       | Habitat                     |
| Schema     | dbo                         |
| Tabla      | `Recaudacion_ServiHabit`    |

## Configuración

Archivo: `config/servihabit/config_servihabit.json`

```json
{
    "tablas_remotas": { "tabla_principal": "Recaudacion_ServiHabit" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" }
}
```
