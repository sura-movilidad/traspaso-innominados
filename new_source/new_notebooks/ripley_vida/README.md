# Ripley_Vida.ipynb

## Propósito

Carga la nómina mensual de recaudación de **seguro de vida Ripley** a la tabla histórica en SQL Server. El notebook selecciona automáticamente el Excel más reciente, separa el RUT y DV del campo combinado `RUT CLIENTE`, convierte la fecha de vencimiento y carga los datos de forma idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                              |
|-------------------|----------------------------------------------------------------------|
| Carpeta           | `data/CCLA/RIPLEY_VIDA/`                                             |
| Formato           | Excel (`.xlsx`)                                                      |
| Hoja preferida    | `Recaudacion` / `Recaudación` / `recaudacion` (fallback: hoja índice 1) |
| Selección         | El archivo con la fecha de modificación más reciente                 |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el Excel

| Columna en el Excel          | Campo en SQL Server   | Notas                                          |
|------------------------------|-----------------------|------------------------------------------------|
| num_cuota / nro_cuota        | `NUM_CUOTA`           |                                                |
| cuota_prima / cuotaprima     | `PRIMA_BRUTA_CLP`     |                                                |
| propuesta_com                | `PROPUESTA_COM`       |                                                |
| prima_bruta_uf               | `PRIMA_BRUTA_UF`      |                                                |
| seguro                       | `SEGURO`              |                                                |
| compania                     | `COMPANIA`            |                                                |
| rut cliente *(texto)*        | `RUT CLIENTE`         | Kept as-is para referencia                     |
| *(extraído de rut cliente)*  | `RUT`                 | Parte numérica (todos menos el último carácter)|
| *(extraído de rut cliente)*  | `DV_RUT`              | Último carácter del campo combinado            |
| fecha_cargo                  | `FECHA_CARGO`         | Convertida a tipo `date`                       |
| fecha vcto *(texto)*         | `FECHA VCTO`          | Convertida a float YYYYMMDD                    |
| vida                         | `VIDA`                |                                                |
| generales                    | `GENERALES`           |                                                |
| *(generado)*                 | `NUMERO_ARCHIVO`      | YYYYMM extraído del nombre del archivo         |
| *(generado)*                 | `NOMBRE_ARCHIVO`      |                                                |

## Transformaciones principales

1. Selección automática del Excel más reciente con manejo de bloqueos OneDrive
2. Normalización de nombres de columna (snake_case, sin tildes)
3. **Separación RUT-DV:** el campo `RUT CLIENTE` (ej. `"12345678K"`) se separa: los caracteres sin el último van a `RUT`, el último a `DV_RUT`
4. **FECHA VCTO:** convertida desde datetime a float YYYYMMDD
5. **FECHA_CARGO:** convertida al tipo `date`
6. Extracción de `NUMERO_ARCHIVO` (YYYYMM) desde el nombre del archivo
7. Columna `NOMBRE_ARCHIVO` se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor                           |
|------------|---------------------------------|
| Servidor   | SGF1034                         |
| Base       | Habitat                         |
| Schema     | dbo                             |
| Tabla      | `Ripley_Recaudacion_Vida`       |

## Configuración

Archivo: `config/ripley_vida/config_ripley_vida.json`

```json
{
    "tablas_remotas": { "tabla_principal": "Ripley_Recaudacion_Vida" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" }
}
```
