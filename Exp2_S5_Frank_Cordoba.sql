SET SERVEROUTPUT ON

/* =========================================================
   Definición de VARIABLE BIND
   Permite ejecutar el proceso para un período específico
   ========================================================= */
VARIABLE b_periodo NUMBER
EXEC :b_periodo := 2026


DECLARE
    /* =========================================================
       Variable BIND – año a procesar
       ========================================================= */
    v_anno_act NUMBER := :b_periodo;

    /* =========================================================
       VARRAY – tipos de transacción válidos según SBIF
       ========================================================= */
    TYPE t_tipos_trans IS VARRAY(2) OF VARCHAR2(40);
    v_tipos t_tipos_trans :=
        t_tipos_trans('Avance en Efectivo','Súper Avance en Efectivo');

    /* =========================================================
       Registro PL/SQL
       ========================================================= */
    TYPE r_detalle IS RECORD (
        numrun        cliente.numrun%TYPE,
        dvrun         cliente.dvrun%TYPE,
        nro_tarjeta   tarjeta_cliente.nro_tarjeta%TYPE,
        nro_tran      transaccion_tarjeta_cliente.nro_transaccion%TYPE,
        fecha_tran    transaccion_tarjeta_cliente.fecha_transaccion%TYPE,
        tipo_tran     tipo_transaccion_tarjeta.nombre_tptran_tarjeta%TYPE,
        monto_tran    transaccion_tarjeta_cliente.monto_transaccion%TYPE,
        porc_aporte   tramo_aporte_sbif.porc_aporte_sbif%TYPE
    );

    v_reg r_detalle;

    /* =========================================================
       Definición de excepciones
       ========================================================= */
    -- Excepción definida por el usuario
    e_aporte_invalido EXCEPTION;

    -- Excepción no predefinida
    e_integridad_ref  EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_integridad_ref, -2292);

    /* =========================================================
       Contadores de control del proceso
       ========================================================= */
    v_total_registros NUMBER := 0;   -- total esperado
    v_procesados      NUMBER := 0;   -- total procesado

    /* =========================================================
       Cursor explícito SIN parámetro
       Obtiene los meses del período a procesar
       ========================================================= */
    CURSOR c_meses IS
        SELECT DISTINCT TO_CHAR(ttc.fecha_transaccion,'MMYYYY')
        FROM transaccion_tarjeta_cliente ttc
        JOIN tipo_transaccion_tarjeta ttt
          ON ttt.cod_tptran_tarjeta = ttc.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_anno_act
          AND ttt.nombre_tptran_tarjeta IN (v_tipos(1), v_tipos(2))
        ORDER BY 1;

    /* =========================================================
       Cursor explícito CON parámetro
       Obtiene el detalle de avances y súper avances por mes
       ========================================================= */
    CURSOR c_det_mes (p_mes VARCHAR2) IS
        SELECT cli.numrun,
               cli.dvrun,
               tcli.nro_tarjeta,
               ttc.nro_transaccion,
               ttc.fecha_transaccion,
               ttt.nombre_tptran_tarjeta,
               ttc.monto_transaccion,
               ap.porc_aporte_sbif
        FROM transaccion_tarjeta_cliente ttc
        JOIN tarjeta_cliente tcli
          ON tcli.nro_tarjeta = ttc.nro_tarjeta
        JOIN cliente cli
          ON cli.numrun = tcli.numrun
        JOIN tipo_transaccion_tarjeta ttt
          ON ttt.cod_tptran_tarjeta = ttc.cod_tptran_tarjeta
        JOIN tramo_aporte_sbif ap
          ON ttc.monto_transaccion
             BETWEEN ap.tramo_inf_av_sav AND ap.tramo_sup_av_sav
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_anno_act
          AND TO_CHAR(ttc.fecha_transaccion,'MMYYYY') = p_mes
          AND ttt.nombre_tptran_tarjeta IN (v_tipos(1), v_tipos(2))
        ORDER BY ttc.fecha_transaccion, cli.numrun;

    v_mes    VARCHAR2(6);  -- mes procesado
    v_aporte NUMBER;       -- aporte SBIF calculado

BEGIN
    /* =========================================================
       Truncado dinámico de tablas de resultados
       Permite ejecutar el proceso múltiples veces
       ========================================================= */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_aporte_sbif';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE resumen_aporte_sbif';

    /* =========================================================
       Obtención del total de registros a procesar
       ========================================================= */
    SELECT COUNT(*)
    INTO v_total_registros
    FROM transaccion_tarjeta_cliente ttc
    JOIN tipo_transaccion_tarjeta ttt
      ON ttt.cod_tptran_tarjeta = ttc.cod_tptran_tarjeta
    JOIN tramo_aporte_sbif ap
      ON ttc.monto_transaccion
         BETWEEN ap.tramo_inf_av_sav AND ap.tramo_sup_av_sav
    WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = v_anno_act
      AND ttt.nombre_tptran_tarjeta IN (v_tipos(1), v_tipos(2));

    /* =========================================================
       Procesamiento principal por mes
       ========================================================= */
    OPEN c_meses;
    LOOP
        FETCH c_meses INTO v_mes;
        EXIT WHEN c_meses%NOTFOUND;

        OPEN c_det_mes(v_mes);
        LOOP
            FETCH c_det_mes INTO v_reg;
            EXIT WHEN c_det_mes%NOTFOUND;

            /* =================================================
               Cálculo del aporte SBIF en PL/SQL
               ================================================= */
            IF v_reg.porc_aporte <= 0 THEN
                RAISE e_aporte_invalido;
            END IF;

            v_aporte :=
                ROUND(v_reg.monto_tran * (v_reg.porc_aporte / 100));

            /* =================================================
               Inserción del detalle de transacciones
               ================================================= */
            INSERT INTO detalle_aporte_sbif
            VALUES (
                v_reg.numrun,
                v_reg.dvrun,
                v_reg.nro_tarjeta,
                v_reg.nro_tran,
                v_reg.fecha_tran,
                v_reg.tipo_tran,
                v_reg.monto_tran,
                v_aporte
            );

            /* =================================================
               Inserción base del resumen mensual
               Manejo de excepción predefinida
               ================================================= */
            BEGIN
                INSERT INTO resumen_aporte_sbif
                VALUES (v_mes, v_reg.tipo_tran, 0, 0);
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN NULL;
            END;

            /* =================================================
               Acumulación de montos y aportes
               ================================================= */
            UPDATE resumen_aporte_sbif
               SET monto_total_transacciones =
                       monto_total_transacciones + v_reg.monto_tran,
                   aporte_total_abif =
                       aporte_total_abif + v_aporte
             WHERE mes_anno = v_mes
               AND tipo_transaccion = v_reg.tipo_tran;

            v_procesados := v_procesados + 1;

        END LOOP;
        CLOSE c_det_mes;
    END LOOP;
    CLOSE c_meses;

    /* =========================================================
       Confirmación de transacciones
       ========================================================= */
    IF v_procesados = v_total_registros THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(
            'Proceso finalizado correctamente. Registros: ' || v_procesados
        );
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE(
            'Rollback ejecutado. Registros incompletos.'
        );
    END IF;

EXCEPTION
    /* =========================================================
       Manejo de excepciones
       ========================================================= */
    WHEN e_aporte_invalido THEN
        DBMS_OUTPUT.PUT_LINE(
            'Error: porcentaje de aporte SBIF inválido.'
        );
        ROLLBACK;

    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE(
            'No existen datos para el período indicado.'
        );
        ROLLBACK;

    WHEN e_integridad_ref THEN
        DBMS_OUTPUT.PUT_LINE(
            'Error de integridad referencial.'
        );
        ROLLBACK;

    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(
            'Error inesperado: ' || SQLERRM
        );
        ROLLBACK;
END;
/

/* VERIFICACIÓN */

SELECT
    numrun                   AS "NUMRUN",
    dvrun                    AS "DV",
    nro_tarjeta              AS "NRO TARJETA",
    nro_transaccion          AS "NRO TRANSACCIÓN",
    fecha_transaccion        AS "FECHA TRANSACCIÓN",
    tipo_transaccion         AS "TIPO TRANSACCIÓN",
    monto_transaccion        AS "MONTO TRANSACCIÓN",
    aporte_sbif              AS "APORTE SBIF"
FROM detalle_aporte_sbif
ORDER BY fecha_transaccion, numrun;


SELECT
    mes_anno                    AS "MES_AÑO",
    tipo_transaccion            AS "TIPO TRANSACCIÓN",
    monto_total_transacciones   AS "MONTO TOTAL TRANSACCIONES",
    aporte_total_abif           AS "APORTE TOTAL SBIF"
FROM resumen_aporte_sbif
ORDER BY mes_anno, tipo_transaccion;

