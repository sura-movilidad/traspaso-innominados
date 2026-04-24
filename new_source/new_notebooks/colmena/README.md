# Colmena.ipynb

## Propósito

Carga la nómina mensual de recaudación del partner **Colmena** a la tabla histórica en SQL Server. Colmena entrega los datos en Excel; el notebook selecciona automáticamente el archivo más reciente, normaliza columnas, mapea el `ID_Producto` a la póliza correspondiente y extrae el período desde el nombre del archivo.

## Archivos de entrada

| Atributo          | Detalle                                                    |
|-------------------|------------------------------------------------------------|
| Carpeta           | `data/CCLA/COLMENA/`                                       |
| Formato           | Excel (`.xlsx`)                                            |
| Hoja preferida    | `MaestroCesantiaPlus` / `maestrocesantiaplus` (fallback: hoja índice 0) |
| Selección         | El archivo con la fecha de modificación más reciente       |

El archivo es eliminado de la carpeta tras una carga exitosa.

## Columnas esperadas en el Excel

| Columna en el Excel       | Campo en SQL Server    | Notas                               |
|---------------------------|------------------------|-------------------------------------|
| n_operacion / folio       | `Guia`                 | Número de operación                 |
| afirut / rut              | `RUT`                  |                                     |
| afirutdv / dv             | `DV`                   |                                     |
| id_producto / idproducto  | `ID_Producto`          | Se mapea a `Poliza` (ver abajo)     |
| producto                  | `Producto`             |                                     |
| fecha_nacimiento          | `Fecha_Nacimiento`     | Convertida a `date`                 |
| fecha_suscripcion         | `Fecha_Suscripcion`    | Convertida a `date`                 |
| costo_total               | `Costo_Total`          |                                     |
| prima_uf                  | `Prima_UF`             |                                     |
| *(generado)*              | `Poliza`               | Mapeado desde ID_Producto           |
| *(generado)*              | `MES_RECAUDACION`      | Extraído del nombre del archivo (YYYYMM) |
| *(generado)*              | `NOMBRE_ARCHIVO`       |                                     |

## Mapeo ID_Producto → Póliza

Definido en `config/colmena/config_colmena.json`:

| ID_Producto | Póliza    |
|-------------|-----------|
| 69          | 7391284   |
| 70          | 7391285   |
| 71          | 7391286   |

## Transformaciones principales

1. Selección automática del archivo Excel más reciente con manejo de bloqueos de OneDrive (copia temporal)
2. Normalización de nombres de columna (snake_case, sin tildes)
3. Mapeo de `ID_Producto` a `Poliza` usando el diccionario del config
4. Extracción de `MES_RECAUDACION` (YYYYMM) desde el nombre del archivo
5. Conversión de fechas (`Fecha_Nacimiento`, `Fecha_Suscripcion`) al tipo `date`
6. Almacenamiento adicional como entero YYYYMMDD en `Fecha_Nac` y `Fecha_Sus`
7. Casteo de montos a float, RUTs a Int64

## Carga a SQL Server

**Carga idempotente:** antes de insertar, verifica si ya existe data con el mismo `NOMBRE_ARCHIVO`. Si existe, elimina las filas previas y carga las nuevas.

| Parámetro  | Valor                    |
|------------|--------------------------|
| Servidor   | SGF1034                  |
| Base       | Habitat                  |
| Schema     | dbo                      |
| Tabla      | `Recaudacion_Colmena`    |

## Configuración

Archivo: `config/colmena/config_colmena.json`

```json
{
    "tablas_remotas": { "tabla_principal": "Recaudacion_Colmena" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" },
    "mapa_polizas":   { "69": 7391284, "70": 7391285, "71": 7391286 }
}
```
