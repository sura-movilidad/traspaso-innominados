# Itau_Fraude.ipynb

## Propósito

Carga la nómina mensual del proceso de **detección de fraude Itaú** a la tabla histórica en SQL Server. Itaú entrega los datos en Excel; el notebook selecciona automáticamente el archivo más reciente, normaliza columnas, estandariza RUTs y fechas con lógica específica para este partner, y carga los datos de forma idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                    |
|-------------------|------------------------------------------------------------|
| Carpeta           | `data/CCLA/ITAU_FRAUDE/`                                   |
| Formato           | Excel (`.xlsx`)                                            |
| Hoja preferida    | `BBDD` / `bbdd` / `BASE` / `base` (fallback: hoja índice 3) |
| Selección         | El archivo con la fecha de modificación más reciente       |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el Excel

| Columna en el Excel        | Campo en SQL Server   | Notas                                    |
|----------------------------|-----------------------|------------------------------------------|
| nro_cliente / num_cliente  | `Cliente`             |                                          |
| rut                        | `RUT`                 | Normalizado (solo dígitos, sin DV)       |
| *(derivado de rut)*        | `DVRUT`               | Último carácter del RUT original         |
| nombre / nombre_cliente    | `NOMBRE`              |                                          |
| fechacobro / fecha_cobro   | `FECHACOBRO`          | Convertida a entero YYYYMMDD             |
| codigo_pago / codigopago   | `CodigoPago`          |                                          |
| poliza                     | `POLIZA`              |                                          |
| kit                        | `KIT`                 |                                          |
| producto                   | `Producto`            |                                          |
| partner_banco              | `PARTNER_BANCO`       |                                          |
| medio_de_pago              | `MediodePago`         |                                          |
| prima_uf                   | `Prima_UF`            |                                          |
| posee_iva                  | `Posee_IVA`           |                                          |
| *(calculado)*              | `Prima_NETA_UF`       | `Prima_UF / 1.19`                        |
| prima_neta                 | `Prima_Neta`          |                                          |
| uf                         | `UF`                  |                                          |
| mes                        | `MES`                 |                                          |
| ano / año                  | `AÑO`                 |                                          |
| vigencia                   | `Vigencia`            | Normalizada a entero YYYYMMDD            |
| fecha_nacimiento           | `Fecha_Nacimiento`    | Normalizada a entero YYYYMMDD (formato mixto) |
| monto                      | `Monto`               |                                          |
| tipo_seguro                | `TIPO_SEGURO`         |                                          |
| mes_ano / mes_año          | `MES_AÑO`             |                                          |
| *(generado)*               | `NOMBRE_ARCHIVO`      |                                          |

## Transformaciones principales

1. Selección automática del Excel más reciente con manejo de bloqueos OneDrive
2. Normalización de nombres de columna (snake_case, sin tildes)
3. **Normalización de RUT:** elimina puntos y guiones, separa el DV en columna `DVRUT`, convierte la parte numérica a `Int64`
4. **Fechas en formato mixto:** la columna `Fecha_Nacimiento` (y `Vigencia`) pueden contener texto `DD/MM/YYYY`, seriales Excel o enteros `YYYYMMDD`; la función `normaliza_fecha_mixta_inplace` los unifica en entero YYYYMMDD
5. **Prima Neta:** calculada como `Prima_UF / 1.19` (divide por factor IVA)
6. Extracción de `MES` y `AÑO` desde el período del archivo
7. Columna `NOMBRE_ARCHIVO` se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor                          |
|------------|--------------------------------|
| Servidor   | SGF1034                        |
| Base       | Habitat                        |
| Schema     | dbo                            |
| Tabla      | `ITAU_INNOMINADO_HISTORICO`    |

## Configuración

Archivo: `config/itau_fraude/config_itau_fraude.json`

```json
{
    "tablas_remotas": { "tabla_principal": "ITAU_INNOMINADO_HISTORICO" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" }
}
```
