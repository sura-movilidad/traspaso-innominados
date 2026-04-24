# Notebooks CCLA — La Araucana Cesantía

Este directorio contiene los notebooks del proceso de **La Araucana (CCLA)** — seguros de cesantía comercializados a través de distintos intermediarios (partners). Cada partner envía sus archivos en un formato propio; los notebooks los estandarizan y cargan a SQL Server para su posterior consolidación y reporte regulatorio.

## Flujo general

```
Archivos Excel por partner
        ↓
Notebook de partner  (Conosur / Marsh / Volvek_*)
        ↓
Tabla acumulada *_ACUMULADO_R_BKP  (carga histórica)
        ↓
Tabla final *_FINAL_ACUMULADO_BKP  (+ MES_Recaudacion / PLAN_TECNICO / POLIZA)
        ↓
Monitoreo_CCLA.ipynb
        ↓
TOTAL_ARAUCANA_BKP → Monitoring_LaAraucana_BKP → Oficio_LA_Araucana_PPI*_BKP
```

---

## Conexión a base de datos

Todos los notebooks se conectan a:

| Parámetro  | Valor                        |
|------------|------------------------------|
| Servidor   | SGF1034                      |
| Base       | Habitat                      |
| Schema     | dbo                          |
| Driver     | ODBC Driver 17 for SQL Server |
| Autenticación | Windows (Trusted Connection) |

---

## Notebooks de carga por partner

### Conosur.ipynb

**Propósito:** Carga la nómina mensual de recaudación de cesantía del partner Conosur hacia la base histórica acumulada.

**Archivos de entrada:**
- Carpeta: `data/CCLA/CONOSUR/`
- Formato: Excel (`.xlsx`)
- Hoja preferida: `Cesantia_Conosur` / `cesantia_conosur` / `conosur_cesantia` (fallback: hoja índice 1)
- Se toma el archivo con la fecha de modificación más reciente
- El archivo se elimina de la carpeta luego de una carga exitosa

**Columnas de entrada esperadas** (nombres tolerantes a variaciones):

| Campo destino       | Nombres de columna aceptados en el Excel           |
|---------------------|----------------------------------------------------|
| foliocredito        | n_operacion, no_operacion, operacion, folio        |
| rutafiliado         | afirut, rut_afiliado, rut                          |
| dvafiliado          | afirutdv, dv_afiliado, dv                          |
| NombreAfiliado      | afinom, nombre_afiliado, nombre                    |
| Plazo               | crecuotot, plazo                                   |
| MontoBruto          | cresolmon, monto_bruto, monto                      |
| fecotorgamiento     | fecotorgamiento, fecha_otorgamiento                |
| fechaPrimerVto      | fecinicob, fecha_primer_vto                        |
| FechaUltimoVto      | fectercob, fecha_ultimo_vto                        |
| ValorCuota          | valcuota, valor_cuota                              |
| FechaPrima          | fecpri, fecha_prima                                |
| Prima               | prima                                              |
| Desgravamen         | desgravamen                                        |
| FechaDefuncion      | fecha_defuncion                                    |
| OrigenDefuncion     | origen_defuncion                                   |
| Producto            | producto                                           |
| FolioOrigen         | folio_origen, folioorigen                          |
| TasaOrigen          | tasa_origen                                        |
| TASA                | tasa                                               |
| FechaOrigen         | fecha_origen                                       |
| POLIZA              | poliza                                             |
| PrimaBrutaMensual   | prima_bruta_mensual                                |
| IVA                 | iva                                                |
| PrimaNetaMensual    | prima_neta_mensual, prima_neta                     |

**Transformaciones principales:**
- Normalización de nombres de columna (snake_case, sin tildes)
- Limpieza de strings (strip, None/nan → vacío)
- Casteo de fechas a entero YYYYMMDD
- Casteo de montos a float, RUTs a Int64
- `Nombre_de_archivo` se genera con `fun.canonicalizar_planes()` a partir del nombre del archivo
- Se eliminan filas al final del DataFrame con >80% de nulos (footers de totales)

**Carga idempotente:** antes de insertar, elimina todas las filas existentes con el mismo `Nombre_de_archivo`.

**Salida:**

| Tabla                        | Descripción                                      |
|------------------------------|--------------------------------------------------|
| `CONOSUR_ACUMULADO_R_BKP`    | Carga histórica del archivo (columnas crudas)    |
| `CONOSUR_FINAL_ACUMULADO_BKP`| Agrega: `MES_Recaudacion`, `PLAN_TECNICO=6832`, `PLAZO_CUOTAS=4`, `Negocio='Credito Consumo'` |

**Configuración:** `config/config_conosur.json`

---

### Marsh.ipynb

**Propósito:** Carga la nómina mensual de recaudación de cesantía del partner Marsh hacia la base histórica acumulada.

**Archivos de entrada:**
- Carpeta: `data/CCLA/MARSH/`
- Formato: Excel (`.xlsx`) — los archivos habitualmente llegan en formato `.xlsb` y deben convertirse a `.xlsx` antes de ejecutar
- Hoja preferida: `base` / `Base cesantia SURA Stock` / `Base` (fallback: hoja índice 0)
- Se toma el archivo con la fecha de modificación más reciente
- El archivo se elimina de la carpeta luego de una carga exitosa

**Columnas de entrada esperadas:** mismas que Conosur, más:

| Campo destino   | Nombres de columna aceptados               |
|-----------------|--------------------------------------------|
| FECHA_DE_VENTA  | fecha_de_venta, fecha_venta                |
| FECHA_DE_BAJA   | fecha_de_baja, fecha_baja                  |
| Corredora       | corredora                                  |

**Transformaciones principales:**
- Mismas que Conosur
- `MES_Recaudacion` se ajusta restando 1 mes (el partner entrega datos con ~2 meses de atraso)

**Carga idempotente:** elimina por `Nombre_de_archivo` antes de insertar.

**Salida:**

| Tabla                       | Descripción                                      |
|-----------------------------|--------------------------------------------------|
| `MARSH_ACUMULADO_R_BKP`     | Carga histórica del archivo (columnas crudas)    |
| `MARSH_FINAL_ACUMULADO_BKP` | Agrega: `MES_Recaudacion` (ajustado -1 mes), `PLAN_TECNICO=8285`, `PLAZO_CUOTAS=4`, `Negocio='Credito Consumo'` |

**Configuración:** `config/config_marsh.json`

---

### Volvek_Flujo.ipynb

**Propósito:** Carga la nómina mensual de recaudación de cesantía del partner Volvek — segmento Flujo (créditos en vigencia, pagos corrientes).

**Archivos de entrada:**
- Carpeta: `data/CCLA/VOLVEK FLUJO/`
- Formato: Excel (`.xlsx`)
- Hoja preferida: `Base Flujo` / `Base cesantia SURA Flujo` / `Base` (fallback: hoja índice 0)

**Columnas de entrada esperadas:** mismo esquema CCLA (foliocredito, rutafiliado, dvafiliado, NombreAfiliado, Plazo, MontoBruto, fechas, primas, etc.) más `Comision25`, `ComisionVariable`.

**Salida:**

| Tabla                         | Descripción                                      |
|-------------------------------|--------------------------------------------------|
| `FLUJO_ACUMULADO_R_BKP` (o `VOLVEK_ACUMULADO_FLUJO`) | Carga histórica |
| `FLUJO_FINAL_ACUMULADO_BKP` (o `FLUJO_FINAL_ACUMULADO`) | Agrega: `POLIZA=4659577`, `MES_Recaudacion`, `PLAN_TECNICO=4277`, `PLAZO_CUOTAS=4`, `Negocio='Credito Consumo'` |

**Configuración:** `config/config_volvek_flujo.json`

---

### Volvek_Flujo2.ipynb

**Propósito:** Carga la nómina mensual del partner Volvek — segmento Flujo 2.0 (segunda generación de pólizas de flujo).

**Archivos de entrada:**
- Carpeta: `data/CCLA/VOLVEK FLUJO 2/`
- Formato: Excel (`.xlsx`)
- Hoja preferida: `Base Flujo 2.0` / `Base cesantia SURA Flujo 2.0` / `Base Flujo 2` (fallback: hoja índice 0)

**Salida:**

| Tabla                          | Descripción                                      |
|--------------------------------|--------------------------------------------------|
| `FLUJO2_ACUMULADO_R_BKP` (o `VOLVEK_ACUMULADO_FLUJO2`) | Carga histórica |
| `FLUJO2_FINAL_ACUMULADO_BKP` (o `FLUJO2_FINAL_ACUMULADO`) | Agrega: `POLIZA=5698774`, `MES_Recaudacion`, `PLAN_TECNICO=6270`, `PLAZO_CUOTAS=4`, `Negocio='Credito Consumo'` |

**Configuración:** `config/config_volver_flujo_2.json`

---

### Volvek_Planes.ipynb

**Propósito:** Carga la nómina mensual del partner Volvek — segmento Planes (pólizas con planes de cuotas variables: 3, 4, 6, 8 o 12 cuotas). La POLIZA y el PLAN_TECNICO se asignan dinámicamente según el valor de la columna `Planes`.

**Archivos de entrada:**
- Carpeta: `data/CCLA/VOLVEK PLANES/`
- Formato: Excel (`.xlsx`)
- Hoja preferida: `Base Planes` / `planes` / `Planes` (fallback: hoja índice 0)

**Transformaciones específicas:** el campo `Planes` del Excel determina POLIZA y PLAN_TECNICO según la tabla:

| Planes               | POLIZA    | PLAN_TECNICO |
|----------------------|-----------|--------------|
| Plan 03/3 Cuotas     | 4780715   | 4331         |
| Plan 04/4 Cuotas     | 4780716   | 4422         |
| Plan 06/6 Cuotas     | 4780717   | 4332         |
| Plan 08/8 Cuotas     | 4780718   | 4333         |
| Plan 12 Cuotas       | 4780719   | 4334         |

**Salida:**

| Tabla                           | Descripción                              |
|---------------------------------|------------------------------------------|
| `PLANES_ACUMULADO_R_BKP`        | Carga histórica                          |
| `PLANES_FINAL_ACUMULADO_BKP`    | Agrega: POLIZA, MES_Recaudacion, PLAN_TECNICO, PLAZO_CUOTAS=4, Negocio |

**Configuración:** `config/config_volvek_planes.json`

---

### Volvek_Stock.ipynb

**Propósito:** Carga la nómina mensual del partner Volvek — segmento Stock (cartera de créditos vigentes acumulados).

**Archivos de entrada:**
- Carpeta: `data/CCLA/VOLVEK STOCK/`
- Formato: Excel (`.xlsx`)
- Hoja preferida: `Base Stock` / `Base cesantia SURA Stock` / `Base` (fallback: hoja índice 0)

**Salida:**

| Tabla                         | Descripción                                      |
|-------------------------------|--------------------------------------------------|
| `STOCK_ACUMULADO_R_BKP` (o `VOLVEK_ACUMULADO_STOCK`) | Carga histórica |
| `STOCK_FINAL_ACUMULADO_BKP` (o `STOCK_FINAL_ACUMULADO`) | Agrega: `POLIZA=4668266`, `MES_Recaudacion`, `PLAN_TECNICO=4277`, `PLAZO_CUOTAS=4`, `Negocio='Credito Consumo'` |

**Configuración:** `config/config_volvek_stock.json`

---

### Monitoreo_CCLA.ipynb

**Propósito:** Notebook de consolidación y reporte regulatorio. Une la data de todos los partners CCLA, calcula comisiones, y genera las tablas de monitoreo y oficio para entrega a la CMF (Comisión para el Mercado Financiero).

**Prerequisitos:** Los notebooks de cada partner deben haberse ejecutado previamente. Este notebook lee directamente desde SQL Server, no desde archivos.

**Tablas de entrada (SQL Server):**
- `MARSH_FINAL_ACUMULADO_BKP`
- `CONOSUR_FINAL_ACUMULADO_BKP`
- `FLUJO_FINAL_ACUMULADO_BKP` / `FLUJO2_FINAL_ACUMULADO_BKP`
- `PLANES_FINAL_ACUMULADO_BKP`
- `STOCK_FINAL_ACUMULADO_BKP`
- `EMISION_DEVENGADA_PPI` (tabla fuente de referencia actuarial)
- `TC_DIARIO` (tipo de cambio diario UF)

**Transformaciones principales:**
1. **UNION ALL** de todos los partners en `TOTAL_ARAUCANA_BKP`
2. **Cálculo de comisiones** según `PLAN_TECNICO`:

   | PLAN_TECNICO        | Tasa de comisión (sobre PrimaNeta) |
   |---------------------|------------------------------------|
   | 4277                | 4.76%                              |
   | 4331, 4332, 4333, 4334, 4422 | 8.74%                   |
   | 6270                | 6.65%                              |
   | 6832, 8285          | 0.87%                              |

3. **Construcción de `Monitoring_LaAraucana_BKP`**: cruce con `EMISION_DEVENGADA_PPI` y `TC_DIARIO` para obtener coberturas, ramos, montos asegurados
4. **Construcción de `Oficio_LA_Araucana_PPI*_BKP`**: tabla para entrega CMF con flags de vigencia, RUT empresa (99017000-2), monto asegurado en CLP

**Salida:**

| Tabla                                | Descripción                                      |
|--------------------------------------|--------------------------------------------------|
| `TOTAL_ARAUCANA_BKP`                 | Consolidado de todos los partners                |
| `Monitoring_LaAraucana_BKP`          | Detalle por cobertura para monitoreo actuarial   |
| `Oficio_LA_Araucana_PPI{YYYYMM}_BKP` | Reporte regulatorio para CMF                    |

**Configuración:** no usa archivo de config; la conexión está hardcodeada (SGF1034 / Habitat).
