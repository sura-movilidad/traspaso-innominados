/*==============================================================
 SCRIPT: Oficio_LA_Araucana_PPI202512
 OBJETIVO:
 - Preparar información de pólizas PPI La Araucana
 - Consolidar primas y determinar vigencia
 - Generar tabla final para Oficio CMF (período 202512)
==============================================================*/


/*--------------------------------------------------------------
 1. CREACIÓN DE TABLA TEMPORAL BASE
    - Extrae información desde Monitoring_LaAraucana
    - Excluye registros agregados (COBERTURA_AGRUP = 'TOTAL')
--------------------------------------------------------------*/
DROP TABLE IF EXISTS #Oficio_LA_Araucana_PPI;

SELECT 
    -- Mes de referencia en formato AAAAMM
    (NANOPROC * 100 + NMESPROC) AS mes_referencia, 

    -- Tipo de documento asegurado (valor fijo)
    1 AS COD_TIPO_DOCUMENTO_ASEGURADO, 

    -- RUT del asegurado
    NNUMCERT AS RUT, 

    -- Número de documento / póliza
    NNUMDOCU, 

    -- Ítem de la póliza
    NNUMITEM, 

    -- Plan técnico del producto
    PLAN_TECNICO,

    -- Código de ramo
    NCORASVS AS COD_RAMO, 

    -- Prima expresada en UF
    PRIMA_UF, 

    -- Periodicidad de pago (valor fijo)
    3 AS COD_PERIODICIDAD_PAGO, 

    -- Tipo de pago (valor fijo)
    9 AS COD_TIPO_PAGO
INTO #Oficio_LA_Araucana_PPI
FROM Monitoring_LaAraucana
WHERE COBERTURA_AGRUP != 'TOTAL';



/*--------------------------------------------------------------
 2. DEPURACIÓN DE REGISTROS POR MES DE CORTE
    - Regla especial para nnumdocu específicos
    - Regla general para el resto de documentos
--------------------------------------------------------------*/
DELETE FROM #Oficio_LA_Araucana_PPI
WHERE 
      nnumdocu IN (6354562 , 7561166) 
      AND mes_referencia < 202411
   OR nnumdocu NOT IN (6354562 , 7561166) 
      AND mes_referencia < 202412;



/*--------------------------------------------------------------
 3. CONSOLIDACIÓN DE INFORMACIÓN
    - Se agrupa por póliza / ítem / plan
    - Se calcula:
        * Último mes informado (MAX mes_referencia)
        * Prima directa acumulada (SUM PRIMA_UF)
--------------------------------------------------------------*/
DROP TABLE IF EXISTS #Oficio_LA_Araucana_PPI2;

SELECT 
    -- Último mes informado por póliza/ítem
    MAX(mes_referencia) AS max_mes_referencia, 

    COD_TIPO_DOCUMENTO_ASEGURADO, 
    RUT, 
    NNUMDOCU, 
    NNUMITEM, 
    PLAN_TECNICO,
    COD_RAMO, 

    -- Prima directa acumulada
    SUM(PRIMA_UF) AS PRIMA_DIRECTA, 

    COD_PERIODICIDAD_PAGO, 
    COD_TIPO_PAGO
INTO #Oficio_LA_Araucana_PPI2
FROM #Oficio_LA_Araucana_PPI
GROUP BY     
    COD_TIPO_DOCUMENTO_ASEGURADO, 
    RUT, 
    NNUMDOCU, 
    NNUMITEM, 
    PLAN_TECNICO,
    COD_RAMO, 
    COD_PERIODICIDAD_PAGO, 
    COD_TIPO_PAGO;



/*--------------------------------------------------------------
 4. CONSTRUCCIÓN DE TABLA FINAL PARA OFICIO CMF
    - Se agregan datos de la compañía
    - Se determina vigencia según mes de corte
    - Se calcula monto asegurado directo en CLP
--------------------------------------------------------------*/
DROP TABLE IF EXISTS Oficio_LA_Araucana_PPI202512;

SELECT 
    -- Identificación de la compañía aseguradora
    99017000 AS RUT_COMPANIA, 
    2 AS DV_RUT_COMPANIA,
    'Seguros Suramericana S.A.' AS NOMBRE_COMPANIA,

    -- Datos consolidados
    a.*, 

    -- RUT del contratante (valor fijo)
    70016160 AS RUT_CONTRATANTE,    
    9 AS DV_CONTRANTE,

    /*----------------------------------------------------------
      Determinación de vigencia:
      - Documentos especiales: vigentes si último mes = 202510
      - Resto: vigentes si último mes = 202511
    ----------------------------------------------------------*/
    CASE 
        WHEN nnumdocu IN (6354562 , 7561166) 
             AND max_mes_referencia = 202510 THEN 1 
        WHEN nnumdocu NOT IN (6354562 , 7561166) 
             AND max_mes_referencia = 202511 THEN 1 
        ELSE 0  
    END AS vigentes,

    /*----------------------------------------------------------
      Cálculo de monto asegurado directo en CLP
      - Depende del plan técnico
      - Se multiplica ValorCuota por un factor fijo
    ----------------------------------------------------------*/
    CASE 
        WHEN a.PLAN_TECNICO = 4331 THEN b.ValorCuota * 3 
        WHEN a.PLAN_TECNICO = 4332 THEN b.ValorCuota * 6
        WHEN a.PLAN_TECNICO = 4333 THEN b.ValorCuota * 8
        WHEN a.PLAN_TECNICO = 4334 THEN b.ValorCuota * 12
        ELSE b.ValorCuota * 4 
    END AS MONTO_ASEGURADO_DIRECTO_CLP,

    -- Subdivisión informada a CMF
    '3B' AS Subdiv, 

    -- Código de ramo CMF
    33 AS RAMO_CMF
INTO Oficio_LA_Araucana_PPI202512
FROM #Oficio_LA_Araucana_PPI2 a
LEFT JOIN TOTAL_ARAUCANA b
    -- Se cruza por mes de recaudación
    ON a.max_mes_referencia = b.MES_Recaudacion
   -- Ítem / folio de crédito
   AND a.NNUMITEM = b.foliocredito
   -- Número de póliza
   AND a.nnumdocu = b.POLIZA;



/*--------------------------------------------------------------
 5. CONSULTA A BASE EXTERNA (LINKED SERVER)
    - Permite validar información de documentos específicos
    - Uso informativo / control
--------------------------------------------------------------*/
SELECT *
FROM OPENQUERY (
    SOL, 
    'SELECT * 
     FROM wrkroyal.ofi113857 
     WHERE DOCUMENTO IN (
        4668266, 4780716, 4780717, 4780718, 4780719,
        5698774, 6354562, 7561166, 4659577, 4780715
     )'
);



/*--------------------------------------------------------------
 6. CONTROL FINAL – RESUMEN DE REGISTROS
    - Cuenta pólizas vigentes vs no vigentes
    - Validación previa al envío CMF
--------------------------------------------------------------*/
SELECT 
    VIGENTES,
    COUNT(NNUMDOCU) AS REGISTROS
FROM Oficio_LA_Araucana_PPI202512
GROUP BY VIGENTES;