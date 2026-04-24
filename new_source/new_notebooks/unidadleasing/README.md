# UnidadLeasing.ipynb

## Propósito

Carga la nómina mensual de recaudación del partner **Unidad de Leasing** a la tabla histórica en SQL Server. Al igual que Los Héroes, Unidad de Leasing entrega **un archivo Excel por póliza**; el notebook procesa todos los archivos presentes en la carpeta del período, elimina filas de totales automáticamente, los consolida y los carga de forma idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                              |
|-------------------|----------------------------------------------------------------------|
| Carpeta           | `data/CCLA/UNIDADLEASING/{PERIODO}/` donde `PERIODO` = YYYYMM      |
| Formato           | Excel (`.xlsx`), múltiples archivos por carpeta                      |
| Hoja              | Hoja de índice 0 (primera hoja)                                      |
| Período activo    | Definido en `config/unidadleasing/config_unidadleasing.json` → campo `periodo` |

Se procesan **todos** los archivos `.xlsx` de la carpeta del período en orden alfabético. Cada archivo corresponde a una póliza distinta.

## Columnas esperadas por archivo Excel

| Columna en el Excel (con variaciones) | Campo en SQL Server   | Notas                                   |
|---------------------------------------|-----------------------|-----------------------------------------|
| unnamed_0                             | `Guia`                | Índice de fila (columna sin nombre)     |
| operacion / operación                 | `Operacion`           | String (puede contener letras)          |
| *(numérico de operacion)*             | `Operacion2`          | Parte numérica, bigint                  |
| oper_unidad / operacion_unidad        | `Oper_Unidad`         | String                                  |
| *(numérico de oper_unidad)*           | `Oper_Unidad2`        | Parte numérica, bigint                  |
| rut_deudor / rut                      | `RUT`                 |                                         |
| dv / dv_deudor                        | `DV`                  |                                         |
| tipo_vivienda / vivienda              | `Tipo_Vivienda`       |                                         |
| comuna                                | `Comuna`              |                                         |
| nro_poliza / poliza                   | `Poliza`              |                                         |
| prima_ces                             | `Prima_Ces`           |                                         |
| prima_bruta_ppi / prima_bruta         | `Prima_Bruta_PPI`     |                                         |
| monto_asegurado                       | `Monto_Asegurado`     |                                         |
| fecha_ctto / fecha_contrato           | `Fecha_Ctto`          | Convertida a `date`                     |
| fecha_vcmto / fecha_vencimiento       | `Fecha_Vcmto`         | Convertida a `date`                     |
| fecha_carga                           | `Fecha_Carga`         | Convertida a `date`                     |
| fecha_nac / fecha_nacimiento          | `Fecha_Nac`           | Convertida a `date`                     |
| primer_vcmto                          | `Primer_Vcmto`        | Convertida a `date`                     |
| *(generado)*                          | `MES_RECAUDACION`     | YYYYMM extraído del nombre del archivo  |
| *(generado)*                          | `NOMBRE_ARCHIVO`      |                                         |

## Transformaciones principales

1. **Iteración multi-archivo:** procesa todos los Excel del período en orden alfabético con `iter_excels()`
2. **Detección automática de footer:** la función `detect_footer_row_index` identifica filas de totales (texto "TOTAL", "sumatoria", etc.) o filas con suma de columnas numéricas, y las elimina
3. Normalización de nombres de columna (snake_case, sin tildes)
4. Separación de `Operacion` y `Oper_Unidad` en versiones string y numéricas (`Operacion2`, `Oper_Unidad2`)
5. Conversión de fechas a tipo `date`
6. Extracción de `MES_RECAUDACION` (YYYYMM) desde el nombre del archivo
7. Columna `NOMBRE_ARCHIVO` se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor                           |
|------------|---------------------------------|
| Servidor   | SGF1034                         |
| Base       | Habitat                         |
| Schema     | dbo                             |
| Tabla      | `UNIDAD_LEASING_RECAUDACION`    |

## Cambiar el período

Para cargar un período distinto, editar el campo `periodo` en el archivo de configuración:

```json
// config/unidadleasing/config_unidadleasing.json
{
    "periodo": "202511"   ← cambiar aquí (YYYYMM)
}
```

## Configuración

Archivo: `config/unidadleasing/config_unidadleasing.json`

```json
{
    "tablas_remotas": { "tabla_principal": "UNIDAD_LEASING_RECAUDACION" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" },
    "periodo":        "202511"
}
```
