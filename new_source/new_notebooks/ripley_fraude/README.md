# Ripley_Fraude.ipynb

## Propósito

Carga la nómina mensual del proceso de **fraude Ripley** a la tabla histórica en SQL Server. Ripley entrega los datos en Excel con RUTs en formato `12.345.678-9`; el notebook los descompone en RUT numérico y DV, calcula la prima neta, extrae el período y carga los datos de forma idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                       |
|-------------------|---------------------------------------------------------------|
| Carpeta           | `data/CCLA/RIPLEY_FRAUDE/`                                    |
| Formato           | Excel (`.xlsx`)                                               |
| Hoja preferida    | `Base` / `base` / `BASE` (fallback: hoja índice 2)            |
| Selección         | El archivo con la fecha de modificación más reciente          |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el Excel

| Columna en el Excel          | Campo en SQL Server   | Notas                                        |
|------------------------------|-----------------------|----------------------------------------------|
| fecha_cargo                  | `FECHA_CARGO`         | Formato datetime, convertida a entero YYYYMMDD |
| num_cuota / nro_cuota        | `NUM_CUOTA`           |                                              |
| cuota_prima                  | `CUOTA_PRIMA`         |                                              |
| prima_uf                     | `PRIMA_UF`            |                                              |
| *(calculado)*                | `PRIMA_NETA_UF`       | `PRIMA_UF / 1.19`                            |
| propuesta_com                | `PROPUESTA_COM`       |                                              |
| poliza                       | `POLIZA`              |                                              |
| rut_deudor                   | `RUT_DEUDOR`          | Parte numérica del RUT del deudor            |
| dv_deudor                    | `DV_DEUDOR`           | Dígito verificador del deudor                |
| rut_cliente                  | `RUT_CLIENTE`         | RUT completo con formato `NNNNNNNN-D`        |
| *(extraído de rut_cliente)*  | `DV_CLIENTE`          | Separado con regex                           |
| seguro                       | `SEGURO`              |                                              |
| compania                     | `COMPANIA`            |                                              |
| observacion                  | `OBSERVACION`         |                                              |
| campaas / campañas           | `CAMPAÑAS`            |                                              |
| fecha_vcto                   | `FECHA_VCTO`          |                                              |
| *(generado)*                 | `ANO_MES_CARGA`       | Extraído del nombre del archivo (YYYYMM)     |
| *(generado)*                 | `NOMBRE_ARCHIVO`      |                                              |

## Transformaciones principales

1. Selección automática del Excel más reciente con manejo de bloqueos OneDrive
2. Normalización de nombres de columna (snake_case, sin tildes)
3. **Separación RUT-DV:** la función `split_rut_dv_hyphen` acepta formatos `12.345.678-9` o `12345678-K` y separa el RUT numérico del DV mediante regex
4. **Prima neta:** `PRIMA_NETA_UF = PRIMA_UF / 1.19`
5. **FECHA_CARGO:** convertida desde formato `YYYY-MM-DD HH:MM:SS` a entero YYYYMMDD
6. Extracción de `ANO_MES_CARGA` (YYYYMM) desde el nombre del archivo
7. Eliminación de fila de totales al final del Excel (función `drop_last_total_strict`)
8. Columna `NOMBRE_ARCHIVO` se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor            |
|------------|------------------|
| Servidor   | SGF1034          |
| Base       | Habitat          |
| Schema     | dbo              |
| Tabla      | `Ripley_Fraude`  |

## Configuración

Archivo: `config/ripley_fraude/config_ripley_fraude.json`

```json
{
    "tablas_remotas": { "tabla_principal": "Ripley_Fraude" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" }
}
```
