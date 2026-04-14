# LosHeroes.ipynb

## Propósito

Carga la nómina mensual de recaudación de cesantía del partner **Los Héroes** a la tabla histórica en SQL Server. A diferencia de otros partners, Los Héroes entrega **un archivo Excel por póliza**; el notebook procesa todos los archivos presentes en la carpeta del período, los consolida y los carga en una sola operación idempotente.

## Archivos de entrada

| Atributo          | Detalle                                                             |
|-------------------|---------------------------------------------------------------------|
| Carpeta           | `data/CCLA/LOSHEROES/{PERIODO}/` donde `PERIODO` = YYYYMM         |
| Formato           | Excel (`.xlsx`), múltiples archivos por carpeta                     |
| Hoja              | Hoja de índice 1 (segunda hoja)                                     |
| Período activo    | Definido en `config/losheroes/config_losheroes.json` → campo `periodo` |

Se procesan **todos** los archivos `.xlsx` de la carpeta del período en orden alfabético. El nombre de cada archivo debe contener el número de póliza (7 dígitos) para que sea extraído automáticamente.

## Columnas esperadas por archivo Excel

| Columna en el Excel (con variaciones)              | Campo en SQL Server   |
|----------------------------------------------------|-----------------------|
| numero_operacion / operacion / n_operacion         | `Numero_Operacion`    |
| rut                                                | `RUT`                 |
| dv_rut / digito_verificador / dv                   | `DV_RUT`              |
| tipo_cliente / tipo_de_cliente                     | `Tipo_Cliente`        |
| prima / prima_bruta                                | `PRIMA`               |
| tipo_moneda / moneda                               | `tipo_Moneda`         |
| nombre_asegurado / asegurado / nombre_cliente      | `Nombre_Asegurado`    |
| prima_neta                                         | `Prima_Neta`          |
| plazo                                              | `Plazo`               |
| tramo                                              | `Tramo`               |
| mes_venta                                          | `Mes_venta`           |
| monto_credito                                      | `Monto_Credito`       |
| monto_cuota                                        | `Monto_Cuota`         |
| *(extraído del nombre del archivo)*                | `Poliza`              |
| *(extraído del nombre de la carpeta)*              | `Mes_Referencia`      |

## Transformaciones principales

1. **Iteración multi-archivo:** procesa todos los Excel del período en orden alfabético
2. **Extracción de póliza:** el número de 7 dígitos se extrae del nombre del archivo (regex)
3. **Extracción de período:** el YYYYMM se extrae del nombre de la carpeta
4. **`coalesce_cols`:** para cada campo acepta múltiples nombres de columna alternativos; retorna la primera que exista
5. Normalización de nombres de columna (snake_case, sin tildes)
6. Validación de RUT (estructura numérica, 6-8 dígitos)
7. Casteo de fechas, montos e importes
8. `Mes_Referencia` (YYYYMM entero) se usa como clave de idempotencia

## Carga a SQL Server

**Carga idempotente:** antes de insertar, elimina todas las filas con el mismo `Mes_Referencia`.

| Parámetro  | Valor                              |
|------------|------------------------------------|
| Servidor   | SGF1034                            |
| Base       | Habitat                            |
| Schema     | dbo                                |
| Tabla      | `HISTORICO_CESANTIA_LosHeroes`     |

## Columnas de salida

```
Mes_Referencia, Poliza, Numero_Operacion, RUT, DV_RUT,
Tipo_Cliente, PRIMA, tipo_Moneda, Nombre_Asegurado, Prima_Neta,
Plazo, Tramo, Mes_venta, Monto_Credito, Monto_Cuota
```

## Cambiar el período

Para cargar un período distinto, editar el campo `periodo` en el archivo de configuración:

```json
// config/losheroes/config_losheroes.json
{
    "periodo": "202511"   ← cambiar aquí (YYYYMM)
}
```

## Configuración

Archivo: `config/losheroes/config_losheroes.json`

```json
{
    "tablas_remotas": { "tabla_principal": "HISTORICO_CESANTIA_LosHeroes" },
    "server_config":  { "server": "SGF1034", "database": "Habitat", "schema": "dbo" },
    "periodo":        "202511"
}
```
