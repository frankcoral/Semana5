Este proyecto implementa un bloque PL/SQL anónimo que genera automáticamente la información requerida por la SBIF sobre Avances en Efectivo y Súper Avances, conforme a la normativa vigente.

El proceso calcula el aporte SBIF según los tramos definidos y almacena los resultados en:

DETALLE_APORTE_SBIF (información detallada)

RESUMEN_APORTE_SBIF (información totalizada por mes y tipo)


Características principales:

Uso de VARIABLE BIND para definir el período a procesar.

Implementación de cursores explícitos (con y sin parámetro).

Uso de VARRAY para tipos de transacción.

Uso de RECORD PL/SQL para procesamiento de datos.

Cálculo del aporte en PL/SQL (no en SELECT).

TRUNCATE dinámico para permitir reejecución del proceso.

Commit condicionado mediante contadores de control.

Manejo de excepciones predefinidas, no predefinidas y definidas por el usuario.


Ejecución:

Definir el período a procesar y ejecutar el bloque:

VARIABLE b_periodo NUMBER
EXEC :b_periodo := 2026


Verificación:

Al finalizar la ejecución, se incluyen consultas SELECT para verificar los datos generados en las tablas de detalle y resumen.
