# Banigualdad.ipynb

## Propósito

Carga la nómina mensual de recaudación del partner **Banigualdad** a la tabla histórica en SQL Server. Banigualdad entrega los datos en formato CSV; el notebook los lee con detección robusta de encoding y separador, los transforma y los inserta de forma idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                    |
|-------------------|------------------------------------------------------------|
| Carpeta           | `data/CCLA/BANIGUALDAD/`                                   |
| Formato           | CSV (`.csv`)                                               |
| Selección         | El archivo con la fecha de modificación más reciente       |
| Encodings probados | `utf-8-sig`, `utf-8`, `latin1`, `cp1252`                 |
| Separadores probados | `;`, `,`, `\t`                                          |
| Nombre esperado   | `remesa_YYYYMMDD.csv` (la fecha se extrae del nombre)      |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el CSV

| Columna en el CSV        | Campo en SQL Server        |
|--------------------------|----------------------------|
| `numero_propuesta`       | `PROPUESTA`                |
| `fecha_de_pago`          | `FECHA_PAGO`               |
| `rut_afiliado`           | `RUTAFILIADO`              |
| `nombre_afiliado`        | `NOMBREAFILIADO`           |
| `n_cuota`                | `CUOTA`                    |
| `valor_cuota_bruto_pesos`| `VALOR_CUOTA_BRUTO_CLP`    |
| *(generado)*             | `NOMBRE_ARCHIVO`           |

## Transformaciones principales

1. Detección automática de encoding y separador (prueba combinaciones hasta encontrar una lectura válida)
2. Normalización de nombres de columna: snake_case, sin tildes, sin caracteres especiales
3. Trim de strings y reemplazo de valores vacíos/`nan`/`None`
4. Extracción del nombre de archivo canónico (calculado desde la fecha en el nombre del CSV)
5. Columna `NOMBRE_ARCHIVO` se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas en la misma transacción.

| Parámetro  | Valor                        |
|------------|------------------------------|
| Servidor   | SGF1034                      |
| Base       | Habitat                      |
| Schema     | dbo                          |
| Tabla      | `BANIGUALDAD_HISTORICO_R`    |

## Configuración

Archivo: `config/banigualdad/config_banigualdad.json`

```json
{
    "tablas_remotas": { "tabla_principal": "BANIGUALDAD_HISTORICO_R" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" }
}
```
