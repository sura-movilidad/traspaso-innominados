/*======================================================================================
 LA ARAUCANA 2.0 - PROCESO MANUAL (BASES XLSX 97) -> CONSOLIDADO -> MONITORING
 ---------------------------------------------------------------------------------------
 OBJETIVO
   1) Revisar/validar bases mensuales cargadas (Marsh/Conosur/Volvek*)
   2) Construir bases "FINAL_ACUMULADO" por corredor / fuente
   3) Consolidar TODO en TOTAL_ARAUCANA
   4) Calcular Comisión
   5) Poblar tabla Monitoring_LaAraucana (detalle + total)
   6) Setear partner final

 CLASIFICACIÓN DE BLOQUES (según tu criterio)
   - MARSH               : MARSH_ACUMULADO_R -> MARSH_FINAL_ACUMULADO
   - CONOSUR             : Conosur_Acumulado_R -> CONOSUR_FINAL_ACUMULADO
   - VOLVEK_FLUJO2        : VOLVEK_ACUMULADO_FLUJO2 -> FLUJO2_FINAL_ACUMULADO
   - VOLVEK_FLUJO         : VOLVEK_ACUMULADO_FLUJO  -> FLUJO_FINAL_ACUMULADO
   - VOLVEK_STOCK         : VOLVEK_ACUMULADO_STOCK  -> STOCK_FINAL_ACUMULADO
   - VOLVEK_PLANES        : VOLVEK_ACUMULADO_Planes -> PLANES_FINAL_ACUMULADO
   - USA TODAS (CONSOLIDA): TOTAL_ARAUCANA + Monitoring_LaAraucana + Joins soporte

 NOTAS IMPORTANTES
   - Las tablas RSA (VOLVEK_ACUMULADO_*_RSA) se mencionan en comentario, pero NO se usan.
   - Requisitos de mes_ref: se obtiene desde Nombre_de_archivo con SUBSTRING(...) = 'YYYYMM'
   - Este script asume SQL Server (por TOP, INTO, DATEADD, CONVERT, etc.)
   - Si la tabla a dropear no existe, DROP TABLE fallará. Si quieres hacerlo "safe":
       IF OBJECT_ID('dbo.TABLA','U') IS NOT NULL DROP TABLE dbo.TABLA;
     (NO lo incluyo para respetar tu script original, pero puedes reemplazarlo.)

 ORDEN DE EJECUCIÓN RECOMENDADO
   0) Validaciones (SELECT TOP / DISTINCT Nombre_de_archivo)
   1) Construcción de bases finales (MARSH/CONOSUR/VOLVEK*)
   2) Consolidación TOTAL_ARAUCANA
   3) Update Comisiones en TOTAL_ARAUCANA
   4) Creación tabla Monitoring_LaAraucana
   5) Insert detalle Monitoring
   6) Insert total Monitoring
   7) Update final Partner

======================================================================================*/


/*======================================================================================
 0) VALIDACIONES / INSPECCIÓN (NO MODIFICA DATOS)
======================================================================================*/

-- [MARSH] Muestra rápida
select top 10 * from MARSH_ACUMULADO_R;

-- [VOLVEK_STOCK] Ejemplo filtro por archivo específico (útil para validar carga)
select *
from VOLVEK_ACUMULADO_STOCK
where Nombre_de_archivo = 'Base cesantia SURA Stock 202508.xlsx';

-- [MARSH] Archivos cargados (desc)
select distinct Nombre_de_archivo
from MARSH_ACUMULADO_R
order by Nombre_de_archivo desc;

-- [CONOSUR] Archivos cargados (desc)
select distinct Nombre_de_archivo
from Conosur_Acumulado_R
order by Nombre_de_archivo desc;

-- [VOLVEK_PLANES] Archivos cargados (desc)
select distinct Nombre_de_archivo
from VOLVEK_ACUMULADO_Planes
order by Nombre_de_archivo desc;

-- [VOLVEK_FLUJO2] Archivos cargados (desc)
select distinct Nombre_de_archivo
from VOLVEK_ACUMULADO_FLUJO2
order by Nombre_de_archivo desc;

-- [VOLVEK_FLUJO] Archivos cargados (desc)
select distinct Nombre_de_archivo
from VOLVEK_ACUMULADO_FLUJO
order by Nombre_de_archivo desc;

-- [VOLVEK_STOCK] Archivos cargados (desc)
select distinct Nombre_de_archivo
from VOLVEK_ACUMULADO_STOCK
order by Nombre_de_archivo desc;



/*======================================================================================
 1) CONSTRUCCIÓN DE BASES FINALES POR CORREDOR / FUENTE
    (Estas son las "staging finales" que luego se unen en TOTAL_ARAUCANA)
======================================================================================*/

----------------------------------------------------------------------------------------
-- 1.A) [MARSH] -> MARSH_FINAL_ACUMULADO
----------------------------------------------------------------------------------------
drop table MARSH_FINAL_ACUMULADO;

select *,
       /* 7561166 as POLIZA, -- comentado en tu script */
       SUBSTRING(Nombre_de_archivo,
                 LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,
                 6) AS MES_Recaudacion,
       8285 as PLAN_TECNICO,
       4 as PLAZO_CUOTAS,
       'Credito Consumo' as Negocio
into MARSH_FINAL_ACUMULADO
from MARSH_ACUMULADO_R;

-- Ajuste provisorio: partner atrasado ~2 meses entre recaudación y real
UPDATE MARSH_FINAL_ACUMULADO
SET MES_Recaudacion = CASE
    WHEN RIGHT(CAST(MES_Recaudacion AS VARCHAR(6)), 2) = '01' THEN (MES_Recaudacion - 89) -- enero -> año-1 mes12
    ELSE (MES_Recaudacion - 1)
END;


----------------------------------------------------------------------------------------
-- 1.B) [CONOSUR] -> CONOSUR_FINAL_ACUMULADO
----------------------------------------------------------------------------------------
drop table CONOSUR_FINAL_ACUMULADO;

select *,
       /* 6354562 as POLIZA, -- comentado en tu script */
       SUBSTRING(Nombre_de_archivo,
                 LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,
                 6) AS MES_Recaudacion,
       6832 as PLAN_TECNICO,
       4 as PLAZO_CUOTAS,
       'Credito Consumo' as Negocio
into CONOSUR_FINAL_ACUMULADO
from Conosur_Acumulado_R;


----------------------------------------------------------------------------------------
-- 1.C) [VOLVEK_FLUJO2] -> FLUJO2_FINAL_ACUMULADO
----------------------------------------------------------------------------------------
drop table FLUJO2_FINAL_ACUMULADO;

select *,
       5698774 as POLIZA,
       SUBSTRING(Nombre_de_archivo,
                 LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,
                 6) AS MES_Recaudacion,
       6270 as PLAN_TECNICO,
       4 as PLAZO_CUOTAS,
       'Credito Consumo' as Negocio
into FLUJO2_FINAL_ACUMULADO
from VOLVEK_ACUMULADO_FLUJO2;


----------------------------------------------------------------------------------------
-- 1.D) [VOLVEK_FLUJO] -> FLUJO_FINAL_ACUMULADO
----------------------------------------------------------------------------------------
drop table FLUJO_FINAL_ACUMULADO;

select *,
       4659577 as POLIZA,
       SUBSTRING(Nombre_de_archivo,
                 LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,
                 6) AS MES_Recaudacion,
       4277 as PLAN_TECNICO,
       4 as PLAZO_CUOTAS,
       'Credito Consumo' as Negocio
into FLUJO_FINAL_ACUMULADO
from VOLVEK_ACUMULADO_FLUJO;


----------------------------------------------------------------------------------------
-- 1.E) [VOLVEK_STOCK] -> STOCK_FINAL_ACUMULADO
----------------------------------------------------------------------------------------
drop table STOCK_FINAL_ACUMULADO;

select *,
       4668266 as POLIZA,
       SUBSTRING(Nombre_de_archivo,
                 LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,
                 6) AS MES_Recaudacion,
       4277 as PLAN_TECNICO,
       4 as PLAZO_CUOTAS,
       'Credito Consumo' as Negocio
into STOCK_FINAL_ACUMULADO
from VOLVEK_ACUMULADO_STOCK;


----------------------------------------------------------------------------------------
-- 1.F) [VOLVEK_PLANES] -> PLANES_FINAL_ACUMULADO
--      (en un mismo archivo existen múltiples planes -> POLIZA/PLAN_TECNICO/PLAZO varían)
----------------------------------------------------------------------------------------
drop table PLANES_FINAL_ACUMULADO;

SELECT *,
       (CASE
            WHEN Planes IN ('Plan 03 Cuotas', 'Plan 3 Cuotas') THEN 4780715
            WHEN Planes IN ('Plan 04 Cuotas', 'Plan 4 Cuotas') THEN 4780716
            WHEN Planes IN ('Plan 06 Cuotas', 'Plan 6 Cuotas') THEN 4780717
            WHEN Planes IN ('Plan 08 Cuotas', 'Plan 8 Cuotas') THEN 4780718
            WHEN Planes = 'Plan 12 Cuotas' THEN 4780719
        END) as POLIZA,
       SUBSTRING(Nombre_de_archivo,
                 LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,
                 6) AS MES_Recaudacion,
       (CASE
            WHEN Planes IN ('Plan 03 Cuotas', 'Plan 3 Cuotas') THEN 4331
            WHEN Planes IN ('Plan 04 Cuotas', 'Plan 4 Cuotas') THEN 4422
            WHEN Planes IN ('Plan 06 Cuotas', 'Plan 6 Cuotas') THEN 4332
            WHEN Planes IN ('Plan 08 Cuotas', 'Plan 8 Cuotas') THEN 4333
            WHEN Planes = 'Plan 12 Cuotas' THEN 4334
        END) as PLAN_TECNICO,
       (CASE
            WHEN Planes IN ('Plan 03 Cuotas', 'Plan 3 Cuotas') THEN 3
            WHEN Planes IN ('Plan 04 Cuotas', 'Plan 4 Cuotas') THEN 4
            WHEN Planes IN ('Plan 06 Cuotas', 'Plan 6 Cuotas') THEN 6
            WHEN Planes IN ('Plan 08 Cuotas', 'Plan 8 Cuotas') THEN 8
            WHEN Planes = 'Plan 12 Cuotas' THEN 12
        END) as PLAZO_CUOTAS,
       'Credito Consumo' as Negocio
INTO PLANES_FINAL_ACUMULADO
FROM VOLVEK_ACUMULADO_Planes;



/*======================================================================================
 2) CONSOLIDACIÓN (USA TODAS): TOTAL_ARAUCANA
    Une Marsh + Conosur + Volvek(Flujo2/Flujo/Stock/Planes) en un dataset único.
======================================================================================*/

drop table TOTAL_ARAUCANA;

SELECT T.*, CONVERT(FLOAT,'0') AS Comision
INTO TOTAL_ARAUCANA
FROM (
    -- [MARSH]
    select MES_Recaudacion, POLIZA, foliocredito, rutafiliado, MontoBruto, Plazo,
           fechaPrimerVto, FechaUltimoVto, ValorCuota, FechaPrima, Producto,
           FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio,
           Prima_Bruta_mensual AS PrimaBruta, Prima_Neta AS PrimaNeta
    from MARSH_FINAL_ACUMULADO

    union all

    -- [CONOSUR]
    select MES_Recaudacion, POLIZA, foliocredito, rutafiliado, MontoBruto, Plazo,
           fechaPrimerVto, FechaUltimoVto, ValorCuota, FechaPrima, Producto,
           FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio,
           PrimaBrutaMensual AS PrimaBruta, PrimaNetaMensual AS PrimaNeta
    from CONOSUR_FINAL_ACUMULADO

    union all

    -- [VOLVEK_FLUJO2]
    select MES_Recaudacion, POLIZA, foliocredito, rutafiliado, MontoBruto, Plazo,
           fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto,
           FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio,
           PrimaBruta, PrimaNeta
    from FLUJO2_FINAL_ACUMULADO

    union all

    -- [VOLVEK_FLUJO]
    select MES_Recaudacion, POLIZA, foliocredito, rutafiliado, MontoBruto, Plazo,
           fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto,
           FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio,
           PrimaBruta, PrimaNeta
    from FLUJO_FINAL_ACUMULADO

    union all

    -- [VOLVEK_STOCK]
    select MES_Recaudacion, POLIZA, foliocredito, rutafiliado, MontoBruto, Plazo,
           fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto,
           FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio,
           PrimaBruta, PrimaNeta
    from STOCK_FINAL_ACUMULADO

    union all

    -- [VOLVEK_PLANES]
    select MES_Recaudacion, POLIZA, foliocredito, rutafiliado, MontoBruto, Plazo,
           fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto,
           FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio,
           PrimaBruta, PrimaNeta
    from PLANES_FINAL_ACUMULADO
) as T;



/*======================================================================================
 3) COMISIONES (USA TODAS): UPDATE SOBRE TOTAL_ARAUCANA
======================================================================================*/

Update TOTAL_ARAUCANA
SET Comision = CASE
    WHEN Plan_Tecnico = 4277 THEN 0.0476 * PrimaNeta
    WHEN Plan_Tecnico IN (4331, 4332, 4333, 4334, 4422) THEN 0.0874 * PrimaNeta
    WHEN Plan_Tecnico = 6270 THEN 0.0665 * PrimaNeta
    WHEN Plan_Tecnico IN (6832, 8285) THEN 0.0087 * PrimaNeta
    ELSE Comision -- mantiene
END;

Print('fin de la base manual');

-- Validación rápida consolidado
select top 10 * from TOTAL_ARAUCANA;



/*======================================================================================
 4) MONITORING (USA TODAS): CREACIÓN TABLA DESTINO
======================================================================================*/

drop table Monitoring_LaAraucana;

CREATE TABLE Monitoring_LaAraucana
(
   NANOPROC NUMERIC(4,0),
   NMESPROC NUMERIC(2,0),
   CCODLNRO VARCHAR(11),
   LN NUMERIC(9,0),
   NNUMDOCU NUMERIC(20,0),
   NNUMENDO BIGINT,
   NNUMITEM BIGINT,
   NNUMCERT BIGINT,
   PROPUESTA BIGINT,
   CCODCOBE VARCHAR(5),
   CCODRAMO VARCHAR(2),
   NCORASVS NUMERIC(5,0),
   PLAN_TECNICO NUMERIC(5,0),
   RAMO_IBNR VARCHAR(50),
   PARTNER_SUCURSAL VARCHAR(100),
   BU VARCHAR(70),
   COBERTURA1 VARCHAR(50),
   COBERTURA2 VARCHAR(80),
   TIPO_COBERTURA CHAR(40),
   CONTRATO_REAS CHAR(50),
   MCA_VIG VARCHAR(2),
   TIPO VARCHAR(6),
   RUT_INT NUMERIC(10,0),
   PARTNER_PYG NVARCHAR(255),
   PLAN_COMERCIAL_GOL NUMERIC(5,0),
   CANAL_GOL NVARCHAR(35),
   SUCURSAL_GOL VARCHAR(60),
   RUT_INTERMEDIARIO_GOL NUMERIC(10,0),
   RUT_EJECUTIVO_GOL NUMERIC(10,0),
   PARTNER_GOL VARCHAR(100),
   BU_PYG NVARCHAR(50),
   ZONA_SUCURSAL NVARCHAR(50),
   RAMO_IBNR_ORIG NVARCHAR(50),
   COBERTURA_AGRUP NVARCHAR(50),
   NFECEMIS DATE,
   NFEINVIG DATE,
   NFETEVIG DATE,
   CCOMOORI VARCHAR(3),
   TC INT,
   TC_UF FLOAT,
   PRIMA FLOAT,
   COMISION FLOAT,
   PRIMA_CLP FLOAT,
   COMISION_CLP FLOAT,
   PRIMA_UF FLOAT,
   COMISION_UF FLOAT
);



/*======================================================================================
 5) MONITORING: INSERT DETALLE (por cobertura real desde EMISION_DEVENGADA_PPI)
     - Fuente: TOTAL_ARAUCANA (consolidado)
     - Enriquecimiento: EMISION_DEVENGADA_PPI (atributos de póliza/plan/cobertura)
     - Periodo: Sura_Periodo (map MES_Recaudacion -> fechas PER / PER_ANT)
     - UF: TC_DIARIO (valor UF por fecha PER)
======================================================================================*/

INSERT into Monitoring_LaAraucana
select
    YEAR(f.PER_ANT) as NANOPROC,
    MONTH(f.PER_ANT) as NMESPROC,
    a.CCODLNRO,
    a.LN,
    b.POLIZA as NNUMDOCU,
    0 as NNUMENDO,
    CONVERT(BIGINT, b.foliocredito) as NNUMITEM,
    b.rutafiliado as NNUMCERT,
    CONVERT(BIGINT, b.foliocredito) as PROPUESTA,
    a.CCODCOBE,
    a.CCODRAMO,
    a.NCORASVS,
    a.PLAN_TECNICO,
    a.RAMO_IBNR,
    a.PARTNER_SUCURSAL,
    a.BU,
    a.COBERTURA1,
    a.COBERTURA2,
    a.TIPO_COBERTURA,
    a.CONTRATO_REAS,
    a.MCA_VIG,
    a.TIPO,
    a.RUT_INT,
    a.PARTNER_PYG,
    a.PLAN_COMERCIAL_GOL,
    a.CANAL_GOL,
    a.SUCURSAL_GOL,
    a.RUT_INTERMEDIARIO_GOL,
    a.RUT_EJECUTIVO_GOL,
    a.PARTNER_GOL,
    a.BU_PYG,
    a.ZONA_SUCURSAL,
    a.RAMO_IBNR_ORIG,
    a.COBERTURA_AGRUP,
    f.PER as NFECEMIS,
    f.PER_ANT as NFEINVIG,
    DATEADD(day, 1, f.PER) AS NFETEVIG,
    'CLP' as CCOMOORI,
    0 as TC,
    c.TC_UF,
    b.PrimaNeta as PRIMA,
    b.Comision * -1 as COMISION,
    b.PrimaNeta as PRIMA_CLP,
    b.Comision * -1 as COMISION_CLP,
    (b.PrimaNeta / c.TC_UF) as PRIMA_UF,
    (b.Comision * -1 / c.TC_UF) as COMISION_UF
from TOTAL_ARAUCANA b
left join (
    select
        NNUMDOCU,
        CCODLNRO,
        LN,
        CCODCOBE,
        CCODRAMO,
        NCORASVS,
        PLAN_TECNICO,
        RAMO_IBNR,
        PARTNER_SUCURSAL,
        BU,
        COBERTURA1,
        COBERTURA2,
        TIPO_COBERTURA,
        CONTRATO_REAS,
        MCA_VIG,
        CCOMOORI,
        TIPO,
        RUT_INT,
        PARTNER_PYG,
        PLAN_COMERCIAL_GOL,
        CANAL_GOL,
        SUCURSAL_GOL,
        RUT_INTERMEDIARIO_GOL,
        RUT_EJECUTIVO_GOL,
        PARTNER_GOL,
        BU_PYG,
        ZONA_SUCURSAL,
        RAMO_IBNR_ORIG,
        COBERTURA_AGRUP,
        count(NNUMDOCU) as casos
    from [Habitat].[dbo].[EMISION_DEVENGADA_PPI]
    where nnumdocu in (4780715, 4780716, 4780717, 4780718, 4780719, 4659577, 4668266, 5698774, 6354562, 7561166)
    group by
        NNUMDOCU, CCODLNRO, LN, CCODCOBE, CCODRAMO, NCORASVS, PLAN_TECNICO, RAMO_IBNR, PARTNER_SUCURSAL,
        BU, COBERTURA1, COBERTURA2, TIPO_COBERTURA, CONTRATO_REAS, MCA_VIG, CCOMOORI, TIPO, RUT_INT,
        PARTNER_PYG, PLAN_COMERCIAL_GOL, CANAL_GOL, SUCURSAL_GOL, RUT_INTERMEDIARIO_GOL,
        RUT_EJECUTIVO_GOL, PARTNER_GOL, BU_PYG, ZONA_SUCURSAL, RAMO_IBNR_ORIG, COBERTURA_AGRUP
) a
    on b.POLIZA = a.NNUMDOCU
left join (select * from Sura_Periodo) f
    on b.MES_Recaudacion = f.MES_REF
left join (select *, CAST(fecha as DATE) as fecha_date from TC_DIARIO) c
    on f.PER = c.fecha_date;



/*======================================================================================
 6) (OPCIONAL) CONSULTA SOPORTE UF
======================================================================================*/
select * from TC_DIARIO;



/*======================================================================================
 7) MONITORING: INSERT TOTAL (agrega fila "TOTAL" por póliza)
     - Reutiliza TOTAL_ARAUCANA, cambia coberturas y codigos a "TOTAL"
======================================================================================*/

Insert Monitoring_LaAraucana
select
    YEAR(f.PER_ANT) as NANOPROC,
    MONTH(f.PER_ANT) as NMESPROC,
    a.CCODLNRO,
    a.LN,
    b.POLIZA as NNUMDOCU,
    0 as NNUMENDO,
    CONVERT(BIGINT, b.foliocredito) as NNUMITEM,
    b.rutafiliado as NNUMCERT,
    CONVERT(BIGINT, b.foliocredito) as PROPUESTA,
    1 as CCODCOBE,
    a.CCODRAMO,
    99 as NCORASVS,
    a.PLAN_TECNICO,
    a.RAMO_IBNR,
    a.PARTNER_SUCURSAL,
    a.BU,
    'TOTAL' as COBERTURA1,
    'TOTAL' as COBERTURA2,
    a.TIPO_COBERTURA,
    a.CONTRATO_REAS,
    a.MCA_VIG,
    a.TIPO,
    a.RUT_INT,
    a.PARTNER_PYG,
    a.PLAN_COMERCIAL_GOL,
    a.CANAL_GOL,
    a.SUCURSAL_GOL,
    a.RUT_INTERMEDIARIO_GOL,
    a.RUT_EJECUTIVO_GOL,
    a.PARTNER_GOL,
    a.BU_PYG,
    a.ZONA_SUCURSAL,
    a.RAMO_IBNR_ORIG,
    'TOTAL' as COBERTURA_AGRUP,
    f.PER as NFECEMIS,
    f.PER_ANT as NFEINVIG,
    DATEADD(day, 1, f.PER) AS NFETEVIG,
    'CLP' as CCOMOORI,
    0 as TC,
    c.TC_UF,
    b.PrimaNeta as PRIMA,
    b.Comision as COMISION,
    b.PrimaNeta as PRIMA_CLP,
    b.Comision * -1 as COMISION_CLP,
    (b.PrimaNeta / c.TC_UF) as PRIMA_UF,
    (b.Comision * -1 / c.TC_UF) as COMISION_UF
from TOTAL_ARAUCANA b
left join (
    select
        NNUMDOCU,
        CCODLNRO,
        LN,
        CCODRAMO,
        PLAN_TECNICO,
        RAMO_IBNR,
        PARTNER_SUCURSAL,
        BU,
        TIPO_COBERTURA,
        CONTRATO_REAS,
        MCA_VIG,
        CCOMOORI,
        TIPO,
        RUT_INT,
        PARTNER_PYG,
        PLAN_COMERCIAL_GOL,
        CANAL_GOL,
        SUCURSAL_GOL,
        RUT_INTERMEDIARIO_GOL,
        RUT_EJECUTIVO_GOL,
        PARTNER_GOL,
        BU_PYG,
        ZONA_SUCURSAL,
        RAMO_IBNR_ORIG,
        count(NNUMDOCU) as casos
    from [Habitat].[dbo].[EMISION_DEVENGADA_PPI]
    where nnumdocu in (4780715, 4780716, 4780717, 4780718, 4780719, 4659577, 4668266, 5698774, 6354562, 7561166)
    group by
        NNUMDOCU, CCODLNRO, LN, CCODRAMO, NCORASVS, PLAN_TECNICO, RAMO_IBNR, PARTNER_SUCURSAL,
        BU, TIPO_COBERTURA, CONTRATO_REAS, MCA_VIG, CCOMOORI, TIPO, RUT_INT,
        PARTNER_PYG, PLAN_COMERCIAL_GOL, CANAL_GOL, SUCURSAL_GOL, RUT_INTERMEDIARIO_GOL,
        RUT_EJECUTIVO_GOL, PARTNER_GOL, BU_PYG, ZONA_SUCURSAL, RAMO_IBNR_ORIG
) a
    on b.POLIZA = a.NNUMDOCU
left join (select * from Sura_Periodo) f
    on b.MES_Recaudacion = f.MES_REF
left join (select *, CAST(fecha as DATE) as fecha_date from TC_DIARIO) c
    on f.PER = c.fecha_date;



/*======================================================================================
 8) MONITORING: UPDATE FINAL PARTNER (USA TODAS)
======================================================================================*/

update Monitoring_LaAraucana
set PARTNER_SUCURSAL = 'La Araucana',
    PARTNER_GOL      = 'La Araucana';


/*======================================================================================
 FIN
======================================================================================*/