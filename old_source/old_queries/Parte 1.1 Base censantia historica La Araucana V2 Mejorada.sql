/* la Araucana 2.0
Actualizar las bases mensuales por formato xlsx 97 en las siguientes tablas:
mantiene el mismo formato, solo debes agregar una columna adiccional que seria el nombre del archivo, 
trata de mantener el mismo formato nombre-(año*100+mes).xlsx dado que sacamos de esta columna el mes_ref

Corredor Marsh tabla MARSH_ACUMULADO_R
corredor Conosur tabla Conosur_Acumulado_R
corredor Volvek tablas 
		VOLVEK_ACUMULADO_FLUJO2
		VOLVEK_ACUMULADO_FLUJO
		VOLVEK_ACUMULADO_Planes
		VOLVEK_ACUMULADO_STOCK

Tablas que estan pero no se consideran para este ejercicio por su antiguedad 2015xx-201712, cuando cambia a Sura desde 201801
VOLVEK_ACUMULADO_FLUJO_RSA
VOLVEK_ACUMULADO_STOCK_RSA
VOLVEK_ACUMULADO_Planes_RSA
*/
select top 10 * from MARSH_ACUMULADO_R

select * from VOLVEK_ACUMULADO_STOCK
where Nombre_de_archivo = 'Base cesantia SURA Stock 202508.xlsx'

select distinct Nombre_de_archivo from MARSH_ACUMULADO_R order by Nombre_de_archivo desc
select distinct Nombre_de_archivo from Conosur_Acumulado_R order by Nombre_de_archivo desc
select distinct Nombre_de_archivo from VOLVEK_ACUMULADO_Planes order by Nombre_de_archivo desc
select distinct Nombre_de_archivo from VOLVEK_ACUMULADO_FLUJO2 order by Nombre_de_archivo desc
select distinct Nombre_de_archivo from VOLVEK_ACUMULADO_FLUJO order by Nombre_de_archivo desc
select distinct Nombre_de_archivo from VOLVEK_ACUMULADO_STOCK order by Nombre_de_archivo desc


drop table MARSH_FINAL_ACUMULADO ;
select *, /*7561166 as POLIZA, */
		SUBSTRING(Nombre_de_archivo, LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5, 6) AS MES_Recaudacion,
		8285 as PLAN_TECNICO,4 as PLAZO_CUOTAS,'Credito Consumo' as Negocio
Into MARSH_FINAL_ACUMULADO
from MARSH_ACUMULADO_R ;

--- ajuste para agregar 2 provisorias segun real de produccion, dado que el partner esta atrasado 2 meses entre la recaudacion y lo real----

UPDATE MARSH_FINAL_ACUMULADO
SET MES_Recaudacion = CASE 
    WHEN RIGHT(CAST(MES_Recaudacion AS VARCHAR(6)), 2) = '01' THEN (MES_Recaudacion - 89)
    ELSE (MES_Recaudacion - 1)
END


/*base final conosur*/
drop table CONOSUR_FINAL_ACUMULADO ;
select *, /*6354562 as POLIZA, */
		SUBSTRING(Nombre_de_archivo, LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5, 6) AS MES_Recaudacion,
		6832 as PLAN_TECNICO,4 as PLAZO_CUOTAS,'Credito Consumo' as Negocio
Into CONOSUR_FINAL_ACUMULADO
from Conosur_Acumulado_R ;

/*base final flujo 2*/
drop table FLUJO2_FINAL_ACUMULADO ;
select *, 5698774 as POLIZA,
		SUBSTRING(Nombre_de_archivo,LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5,6) AS MES_Recaudacion,
		6270 as PLAN_TECNICO,4 as PLAZO_CUOTAS,'Credito Consumo' as Negocio
Into FLUJO2_FINAL_ACUMULADO
from VOLVEK_ACUMULADO_FLUJO2 ;

/*base final plan flujo*/
drop table FLUJO_FINAL_ACUMULADO ;
select *,4659577 as POLIZA,
		SUBSTRING(Nombre_de_archivo,LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5, 6) AS MES_Recaudacion,
		4277 as PLAN_TECNICO,4 as PLAZO_CUOTAS,'Credito Consumo' as Negocio
Into FLUJO_FINAL_ACUMULADO
from VOLVEK_ACUMULADO_FLUJO 

drop table STOCK_FINAL_ACUMULADO ;
select *, 4668266 as POLIZA,
		SUBSTRING(Nombre_de_archivo, LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5, 6) AS MES_Recaudacion,
		4277 as PLAN_TECNICO,4 as PLAZO_CUOTAS,'Credito Consumo' as Negocio
Into STOCK_FINAL_ACUMULADO
from VOLVEK_ACUMULADO_STOCK 

/*Planes es mas complicado en un mismo archivo hay distintas polizas y varian por plan */
drop table PLANES_FINAL_ACUMULADO 
SELECT * ,  (CASE 
        WHEN Planes IN ('Plan 03 Cuotas', 'Plan 3 Cuotas') THEN 4780715
        WHEN Planes IN ('Plan 04 Cuotas', 'Plan 4 Cuotas') THEN 4780716
        WHEN Planes IN ('Plan 06 Cuotas', 'Plan 6 Cuotas') THEN 4780717
        WHEN Planes IN ('Plan 08 Cuotas', 'Plan 8 Cuotas') THEN 4780718
        WHEN Planes = 'Plan 12 Cuotas' THEN 4780719
    END) as POLIZA, 
	SUBSTRING( Nombre_de_archivo, LEN(Nombre_de_archivo) - CHARINDEX('.', REVERSE(Nombre_de_archivo)) - 5, 6) AS MES_Recaudacion,
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
FROM 
    VOLVEK_ACUMULADO_Planes

drop table TOTAL_ARAUCANA

SELECT T.*, CONVERT(FLOAT,'0') AS Comision
INTO TOTAL_ARAUCANA
FROM(
select MES_Recaudacion, POLIZA, foliocredito, rutafiliado,MontoBruto, Plazo, fechaPrimerVto, FechaUltimoVto, ValorCuota, FechaPrima, Producto, FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio, Prima_Bruta_mensual AS PrimaBruta, Prima_Neta AS PrimaNeta
from MARSH_FINAL_ACUMULADO
union all 
select MES_Recaudacion, POLIZA, foliocredito, rutafiliado,MontoBruto, Plazo, fechaPrimerVto, FechaUltimoVto, ValorCuota, FechaPrima, Producto, FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio, PrimaBrutaMensual AS PrimaBruta, PrimaNetaMensual AS PrimaNeta
from CONOSUR_FINAL_ACUMULADO
union all
select MES_Recaudacion, POLIZA, foliocredito, rutafiliado,MontoBruto, Plazo, fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto, FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio, PrimaBruta, PrimaNeta
from FLUJO2_FINAL_ACUMULADO
union all
select MES_Recaudacion, POLIZA, foliocredito, rutafiliado,MontoBruto, Plazo, fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto, FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio, PrimaBruta, PrimaNeta
from FLUJO_FINAL_ACUMULADO
union all
select MES_Recaudacion, POLIZA, foliocredito, rutafiliado,MontoBruto, Plazo, fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto, FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio, PrimaBruta, PrimaNeta
from STOCK_FINAL_ACUMULADO
union all
select MES_Recaudacion, POLIZA, foliocredito, rutafiliado,MontoBruto, Plazo, fechaPrimerVto, FechaUltimoVto, ValorCuota, NULL as FechaPrima, Producto, FolioOrigen, FechaOrigen, PLAN_TECNICO, PLAZO_CUOTAS, Negocio, PrimaBruta, PrimaNeta
from PLANES_FINAL_ACUMULADO) as T

Update TOTAL_ARAUCANA
SET Comision = CASE 
    WHEN Plan_Tecnico = 4277 THEN 0.0476*PrimaNeta
    WHEN Plan_Tecnico = 4331 THEN 0.0874*PrimaNeta
    WHEN Plan_Tecnico = 4332 THEN 0.0874*PrimaNeta
    WHEN Plan_Tecnico = 4333 THEN 0.0874*PrimaNeta
    WHEN Plan_Tecnico = 4334 THEN 0.0874*PrimaNeta
    WHEN Plan_Tecnico = 4422 THEN 0.0874*PrimaNeta
    WHEN Plan_Tecnico = 6270 THEN 0.0665*PrimaNeta
    WHEN Plan_Tecnico = 6832 THEN 0.0087*PrimaNeta
    WHEN Plan_Tecnico = 8285 THEN 0.0087*PrimaNeta
    ELSE Comision -- Mantener el valor actual si no coincide con ningún Plan_Tecnico
END;
Print('fin de la base manual')


select top 10 *  from TOTAL_ARAUCANA
/* Con esta base tenemos lo necesario para armar el proceso de monitoring*/

drop table Monitoring_LaAraucana 
CREATE TABLE Monitoring_LaAraucana
(
   NANOPROC NUMERIC(4,0),NMESPROC NUMERIC(2,0),
   CCODLNRO VARCHAR(11),LN NUMERIC(9,0),
   NNUMDOCU NUMERIC(20,0),NNUMENDO BIGINT,
   NNUMITEM BIGINT,NNUMCERT BIGINT, PROPUESTA BIGINT,
   CCODCOBE VARCHAR(5), CCODRAMO VARCHAR(2),
   NCORASVS NUMERIC(5,0),PLAN_TECNICO NUMERIC(5,0),
   RAMO_IBNR VARCHAR(50),PARTNER_SUCURSAL VARCHAR(100),
   BU VARCHAR(70),COBERTURA1 VARCHAR(50),
   COBERTURA2 VARCHAR(80),TIPO_COBERTURA CHAR(40),
   CONTRATO_REAS CHAR(50),MCA_VIG VARCHAR(2),
   TIPO VARCHAR(6),RUT_INT NUMERIC(10,0),
   PARTNER_PYG NVARCHAR(255),PLAN_COMERCIAL_GOL NUMERIC(5,0),
   CANAL_GOL NVARCHAR(35),SUCURSAL_GOL VARCHAR(60),
   RUT_INTERMEDIARIO_GOL NUMERIC(10,0),RUT_EJECUTIVO_GOL NUMERIC(10,0),PARTNER_GOL VARCHAR(100),
   BU_PYG NVARCHAR(50),ZONA_SUCURSAL NVARCHAR(50),RAMO_IBNR_ORIG NVARCHAR(50),
   COBERTURA_AGRUP NVARCHAR(50),
   NFECEMIS DATE,NFEINVIG DATE,NFETEVIG DATE,
   CCOMOORI VARCHAR(3),TC INT,TC_UF FLOAT,
   PRIMA FLOAT,COMISION FLOAT,PRIMA_CLP FLOAT,
   COMISION_CLP FLOAT,PRIMA_UF FLOAT,COMISION_UF FLOAT,
)


/* si queremos la base por cantidad de items por cantidad de tarjeta se usa el N_OPERACION;  CONVERT(BIGINT, b.N_OPERACION)
si queremos la base de datos con solo la infomracion del asegurado por plan tecnico usemos items rut asegurado.*/

INSERT into Monitoring_LaAraucana
select YEAR(f.PER_ANT) as NANOPROC, MONTH(f.PER_ANT) as [NMESPROC],a.CCODLNRO, a.LN, 
			b.POLIZA as NNUMDOCU, 0 as NNUMENDO, CONVERT(BIGINT, b.foliocredito) as NNUMITEM,
			b.rutafiliado as NNUMCERT, CONVERT(BIGINT, b.foliocredito) as PROPUESTA,
			a.CCODCOBE, a.CCODRAMO, a.NCORASVS, a.PLAN_TECNICO, a.RAMO_IBNR, 
			a.PARTNER_SUCURSAL, a.BU, a.COBERTURA1, a.COBERTURA2, a.TIPO_COBERTURA, 
			a.CONTRATO_REAS, a.MCA_VIG, a.TIPO, a.RUT_INT, a.PARTNER_PYG, a.PLAN_COMERCIAL_GOL,
			a.CANAL_GOL, a.SUCURSAL_GOL, a.RUT_INTERMEDIARIO_GOL, a.RUT_EJECUTIVO_GOL, 
			a.PARTNER_GOL, a.BU_PYG, a.ZONA_SUCURSAL, a.RAMO_IBNR_ORIG, a.COBERTURA_AGRUP, 
			F.PER as NFECEMIS, F.PER_ANT as NFEINVIG, DATEADD(day, 1, F.PER) AS NFETEVIG, 
			'CLP' as CCOMOORI, 0 as TC, c.TC_UF, 
			b.PrimaNeta as PRIMA, b.Comision*-1 as COMISION, 
			 b.PrimaNeta as PRIMA_CLP, b.Comision*-1 as COMISION_CLP, 
			 (b.PrimaNeta/c.TC_UF) as PRIMA_UF, (b.Comision*-1 /c.TC_UF)  as COMISION_UF 

from TOTAL_ARAUCANA b
left join (select NNUMDOCU,CCODLNRO,LN, CCODCOBE, CCODRAMO, NCORASVS, PLAN_TECNICO, RAMO_IBNR, PARTNER_SUCURSAL, 
				BU, COBERTURA1, COBERTURA2, TIPO_COBERTURA, CONTRATO_REAS, MCA_VIG, CCOMOORI, TIPO, RUT_INT, 
				PARTNER_PYG, PLAN_COMERCIAL_GOL, CANAL_GOL, SUCURSAL_GOL, 		RUT_INTERMEDIARIO_GOL,
				RUT_EJECUTIVO_GOL, PARTNER_GOL, BU_PYG, ZONA_SUCURSAL, RAMO_IBNR_ORIG, 
				COBERTURA_AGRUP, count(NNUMDOCU) as casos
			from [Habitat].[dbo].[EMISION_DEVENGADA_PPI]
		where nnumdocu in (4780715, 4780716, 4780717, 4780718, 4780719, 4659577, 4668266, 5698774, 6354562, 7561166)
		group by NNUMDOCU, CCODLNRO, LN, CCODCOBE, CCODRAMO, NCORASVS, PLAN_TECNICO, RAMO_IBNR, PARTNER_SUCURSAL, 
				BU, COBERTURA1, COBERTURA2, TIPO_COBERTURA, CONTRATO_REAS, MCA_VIG, CCOMOORI, TIPO, RUT_INT, 
				PARTNER_PYG, PLAN_COMERCIAL_GOL, CANAL_GOL, SUCURSAL_GOL, 		RUT_INTERMEDIARIO_GOL,
				RUT_EJECUTIVO_GOL, PARTNER_GOL, BU_PYG, ZONA_SUCURSAL, RAMO_IBNR_ORIG,COBERTURA_AGRUP) a
on b.POLIZA = a.NNUMDOCU 
left JOIN (select * from Sura_Periodo) f
on b.MES_Recaudacion = f.MES_REF
left join  (select *, CAST(fecha as DATE) as fecha_date from TC_DIARIO) c
on F.PER = c.fecha_date

select * from TC_DIARIO
/*total */

Insert Monitoring_LaAraucana
select YEAR(f.PER_ANT) as NANOPROC, MONTH(f.PER_ANT) as [NMESPROC],a.CCODLNRO, a.LN, 
			b.POLIZA as NNUMDOCU, 0 as NNUMENDO, CONVERT(BIGINT, b.foliocredito) as NNUMITEM,
			b.rutafiliado as NNUMCERT, CONVERT(BIGINT, b.foliocredito) as PROPUESTA,
			1 as CCODCOBE, a.CCODRAMO, 99 as NCORASVS, a.PLAN_TECNICO, a.RAMO_IBNR, 
			a.PARTNER_SUCURSAL, a.BU, 'TOTAL' as COBERTURA1, 'TOTAL' as COBERTURA2, a.TIPO_COBERTURA, 
			a.CONTRATO_REAS, a.MCA_VIG, a.TIPO, a.RUT_INT, a.PARTNER_PYG, a.PLAN_COMERCIAL_GOL,
			a.CANAL_GOL, a.SUCURSAL_GOL, a.RUT_INTERMEDIARIO_GOL, a.RUT_EJECUTIVO_GOL, 
			a.PARTNER_GOL, a.BU_PYG, a.ZONA_SUCURSAL, a.RAMO_IBNR_ORIG, 'TOTAL' as COBERTURA_AGRUP, 
			F.PER as NFECEMIS, F.PER_ANT as NFEINVIG, DATEADD(day, 1, F.PER) AS NFETEVIG, 
			'CLP' as CCOMOORI, 0 as TC, c.TC_UF, 
			b.PrimaNeta as PRIMA, b.Comision as COMISION,
			 b.PrimaNeta as PRIMA_CLP, b.Comision*-1 as COMISION_CLP,
			 (b.PrimaNeta/c.TC_UF) as PRIMA_UF, (b.Comision*-1 /c.TC_UF)  as COMISION_UF

from TOTAL_ARAUCANA b
left join (select NNUMDOCU,CCODLNRO,LN, CCODRAMO, PLAN_TECNICO, RAMO_IBNR, PARTNER_SUCURSAL, 
				BU, TIPO_COBERTURA, CONTRATO_REAS, MCA_VIG, CCOMOORI, TIPO, RUT_INT, 
				PARTNER_PYG, PLAN_COMERCIAL_GOL, CANAL_GOL, SUCURSAL_GOL, 		RUT_INTERMEDIARIO_GOL,
				RUT_EJECUTIVO_GOL, PARTNER_GOL, BU_PYG, ZONA_SUCURSAL, RAMO_IBNR_ORIG, 
				count(NNUMDOCU) as casos
			from [Habitat].[dbo].[EMISION_DEVENGADA_PPI]
		where nnumdocu in (4780715, 4780716, 4780717, 4780718, 4780719, 4659577, 4668266, 5698774, 6354562, 7561166)
		group by NNUMDOCU, CCODLNRO, LN, CCODRAMO, NCORASVS, PLAN_TECNICO, RAMO_IBNR, PARTNER_SUCURSAL, 
				BU, TIPO_COBERTURA, CONTRATO_REAS, MCA_VIG, CCOMOORI, TIPO, RUT_INT, 
				PARTNER_PYG, PLAN_COMERCIAL_GOL, CANAL_GOL, SUCURSAL_GOL, 		RUT_INTERMEDIARIO_GOL,
				RUT_EJECUTIVO_GOL, PARTNER_GOL, BU_PYG, ZONA_SUCURSAL, RAMO_IBNR_ORIG) a
on b.POLIZA = a.NNUMDOCU 
left JOIN (select * from Sura_Periodo) f
on b.MES_Recaudacion = f.MES_REF
left join  (select *, CAST(fecha as DATE) as fecha_date from TC_DIARIO) c
on F.PER = c.fecha_date


update Monitoring_LaAraucana
set PARTNER_SUCURSAL = 'La Araucana',
 PARTNER_GOL = 'La Araucana';
