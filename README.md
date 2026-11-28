# UrbanFlow

Portal de movilidad para explorar recogidas históricas de taxis amarillos en Nueva York y evaluar estimaciones generadas con Spark MLlib. El portal privado muestra el estado de los workers y ejecuta cálculos de corredores por mes en YARN.

## Lanzador gráfico en Windows

Haz doble clic en `UrbanFlow.cmd`. El lanzador permite iniciar sesión en AWS, desplegar uno o seis meses, abrir los paneles, consultar el estado, respaldar SQLite y eliminar los recursos sin escribir comandos. Las acciones de AWS abren una consola visible para mostrar su progreso y sus errores.

## Motor periódico distribuido en AWS

La reproducción por lotes se ejecuta en el servicio `urbanflow-worker` del maestro EC2. Este programa corridas reales de Spark sobre YARN (tres ejecutores configurados), sin depender de que haya un navegador abierto. No crea una cuarta máquina ni ejecuta Spark en el computador del operador.

1. Despliega o actualiza con `urbanflow.ps1 portal` y los meses deseados. Antes de actualizar, detén la programación y espera a que no haya trabajos pendientes o corriendo.
2. Abre el panel administrativo mediante `urbanflow.ps1 admin`. En **Motor distribuido en ejecución**, indica meses, primer corte histórico, último corte e intervalo.
3. Ejemplo de exposición: meses `2025-01,2025-02,2025-03,2025-04,2025-05,2025-06`, primer corte `2025-06-28`, último `2025-06-30`, intervalo de 10 minutos. Con un mes de junio también es válido; seis meses aumentan el volumen real procesado. El primer corte exige al menos 28 días de historia.
4. Pulsa **Programar motor en AWS**. La primera corrida queda disponible inmediatamente; las siguientes esperan el intervalo **después** de finalizar la anterior. Si dura 15 minutos y el intervalo es 10, el siguiente inicio será unos 25 minutos después del primero. No hay superposición ni garantía de rendimiento sin medición.
5. **Adelantar próximo lote** evita la espera cuando el motor está entre corridas. **Detener programación** impide corridas futuras y deja terminar la actual.

Cada corrida avanza un día y ejecuta ingesta HDFS, ETL limitado al corte, perfil estadístico, evaluación temporal/entrenamiento MLlib y Spark SQL. Es procesamiento batch acumulativo con una ventana histórica creciente; **no** es Structured Streaming ni ETL incremental que lea solo los registros nuevos. Los Parquet pueden estar ya descargados, pero ninguna fila posterior al corte entra en agregados ni entrenamiento.

La elección de Random Forest queda fijada antes de evaluar, en lugar de escoger al ganador según la prueba. Tras evaluar, se vuelve a ajustar RF con todas las observaciones disponibles hasta el corte para producir el pronóstico del día siguiente. Se conserva el promedio estadístico como comparación. No se implementa todavía un horizonte semanal.

El motor guarda los modelos por corrida en HDFS (`/urbanflow/modelos/replay/ID`), los logs/CSV por trabajo y el historial de pronósticos en SQLite (`forecast_history`). Al llegar el lote real del día pronosticado, guarda sus conteos (`replay_observations`) y calcula el error contra los pronósticos previos. Nunca usa una estimación ni una solicitud sintética como etiqueta. No hay evaluación del último día pronosticado si el histórico termina antes de que llegue su observación.

Antes de eliminar el clúster, `urbanflow.ps1 respaldo` descarga una copia consistente de SQLite. Para conservar también modelos y resultados, `scripts/backup_evidence.sh` se ejecuta como usuario `hadoop` en el maestro después de terminar los trabajos: genera un `.tgz` con los modelos de reproducción y los CSV/logs de las corridas. Descarga ese archivo al computador; crear el paquete únicamente en EC2 no lo protege de la autodestrucción.

El panel consulta el estado cada tres segundos: corte, etapa, aplicación YARN, próxima corrida, pronósticos publicados y errores contra lotes posteriores. El portal público detecta una publicación nueva y actualiza las predicciones cada 15 segundos; si el usuario estaba mirando el último día, sigue el nuevo pronóstico.

La configuración y la cola sobreviven al reinicio del proceso. Un fallo pausa el programa para revisión. El modo local muestra este monitor, pero no habilita corridas distribuidas. Las instancias deben permanecer encendidas: cerrar el navegador no detiene el motor, pero eliminar/terminar EC2 sí. El temporizador de autodestrucción sigue vigente, y mientras el clúster esté encendido consume los recursos de AWS de la cuenta.

El lanzamiento prepara el motor; la reproducción empieza al programarla en el panel. La actualización no habilita automáticamente un bucle de trabajo con costo. Las pruebas locales validan el programador, la publicación transaccional y la separación de observaciones/pronósticos; la validación completa de Spark/YARN requiere una corrida en el clúster.

## Laboratorio local: modelos y solicitudes persistentes

Haz doble clic en `Demo-local.cmd`, o elige **Laboratorio local (sin AWS)** en `UrbanFlow.cmd`.
Abre http://127.0.0.1:8090/laboratory y deja abierta la consola del servidor.
Requiere Python y las dos dependencias de `portal/requirements.txt` (el comando las instala si faltan).
Si el puerto 8090 está ocupado por un túnel de AWS, ciérralo antes o ejecuta `python -m portal.local_demo --port 8091`.

La primera ejecución restaura el último respaldo de `estado/respaldos` en `runtime/demo/urbanflow.sqlite3`.
Las siguientes reutilizan esa base y conservan las solicitudes y modelos. El respaldo original no se modifica.

1. Pulsa **Entrenar y evaluar con datos reales**. Se crea una versión en `lab_models` con parámetros, huella SHA-256 y métricas.
2. Las primeras 70% de fechas completas de `predictions.observed` entrenan un promedio por zona/hora y una regresión Ridge; las fechas restantes evalúan ambos modelos congelados. La penalización de Ridge se fija antes de evaluar. No se elige un ganador ni se ajustan parámetros con la prueba.
3. Pulsa **Iniciar peticiones**. El navegador envía una petición HTTP por segundo, hasta el límite elegido. Cerrar la página o pulsar Detener termina la generación; una petición en tránsito puede terminar de guardarse.
4. Recarga la página o reinicia el servidor: las filas siguen en SQLite. `GET /api/lab` permite inspeccionarlas como JSON.

Cada `POST /api/lab/requests` guarda un escenario sintético de una zona-hora, con semilla, clave idempotente, versión del modelo, estimación fija, estimación ML, conteo simulado y enriquecimiento de calendario. No representa una solicitud real de taxi individual. La generación usa el promedio estadístico congelado y una variación log-normal explícita (sigma=0.3), no realimenta las predicciones ML.

`lab_requests.source` siempre es `synthetic` y `observed` siempre es NULL, también por restricciones de SQLite. El entrenamiento consulta exclusivamente observaciones históricas de `predictions`, nunca solicitudes sintéticas, perfiles estimados ni la columna `estimate`. No se calcula acierto contra conteos simulados. La comparación local reutiliza las observaciones publicadas por Spark, pero sus particiones y métricas son un experimento diferente de las métricas históricas de Random Forest/GBT; no deben compararse directamente.

El reentrenamiento es explícito y versionado: tras publicar nuevas observaciones reales mediante el pipeline de Spark, puede repetirse. Más solicitudes simuladas no equivalen a más observaciones reales ni mejoran automáticamente el modelo. Esta entrega no incorpora datos actuales de clima ni una fuente real en vivo.

Para demostrarlo ante el profesor: muestra la comparación temporal, inicia 30 peticiones, observa sus IDs, abre `/api/lab`, reinicia y comprueba la persistencia. Las tablas auditables son `lab_models` y `lab_requests`; el respaldo SQLite incluye ambas.

## Justificación y límites de Big Data

Los millones de viajes comprimidos son una fuente real para ETL, agregaciones, ventanas temporales, grafos y MLlib. El laboratorio de solicitudes funciona en una máquina; su existencia no demuestra por sí sola necesidad de Hadoop. Tampoco deben contarse las tres réplicas HDFS como tres veces más información ni presentar simulaciones como crecimiento real del histórico.

La justificación experimental pendiente es medir el mismo ETL/modelo sobre uno, tres y seis meses reales, comparando uno y tres ejecutores, con iguales reglas y parámetros. Registra bytes lógicos, viajes válidos, duración, memoria y tiempo de entrenamiento. Los scripts actuales permiten seleccionar hasta seis meses; no se ejecutó un nuevo benchmark distribuido por añadir este laboratorio. Las métricas de una comparación deben salir de ejecuciones medidas, no de estimaciones de escalabilidad.

## Despliegue temporal en AWS

Desde Windows, abre PowerShell en la raíz del repositorio. Necesitas AWS CLI con sesión activa, OpenSSH, tar y permisos para operar el clúster EC2 en la región elegida. El clúster usa cuatro instancias y discos. Se factura conforme a tu cuenta y región.

```powershell
aws sts get-caller-identity
.\urbanflow.ps1 portal -Meses 2025-06 -HorasMaximas 4
```

El comando crea o reutiliza el clúster etiquetado `urbanflow-portal`, instala el portal y encola la preparación. La URL HTTPS de movilidad aparece en la terminal y puede abrirse desde cualquier navegador. El puerto del servidor de origen deja de aceptar conexiones directas cuando CloudFront supera la comprobación de salud.

El portal de movilidad usa HTTPS mediante el dominio temporal `*.cloudfront.net` asignado por AWS y expone datos públicos de lectura. CloudFront desactiva la caché para que la API siempre consulte la publicación vigente y añade encabezados de seguridad. El portal de operación va por un túnel SSH. Abre una segunda terminal y mantén ese comando en ejecución.

```powershell
.\urbanflow.ps1 admin
```

Abre [Operación de UrbanFlow](http://127.0.0.1:8090). Sigue las etapas de ingesta, limpieza, perfil, MLlib y Spark SQL. Tras la publicación, filtra el portal de movilidad y prueba **Recalcular corredores** desde operación. El portal muestra el mes y el ID de trabajo de la última publicación. Los primeros resultados requieren que termine la preparación en AWS.

Después de la exposición, elimina CloudFront y el clúster y verifica el mensaje de limpieza.

```powershell
.\urbanflow.ps1 eliminar
```

Usa la misma región y perfil de AWS en todos los comandos. El temporizador termina las instancias. El comando de eliminación retira también la red. El nombre `urbanflow-portal` aísla este despliegue del clúster de la versión anterior. No ejecutes los comandos heredados de procesamiento sobre el mismo clúster mientras esté corriendo el portal, ya que usan las mismas rutas de HDFS.

## Estructura

| Ruta | Responsabilidad |
|---|---|
| `portal/app.py` | API ASGI de movilidad y administración. Solo la instancia privada expone la API de operación. |
| `portal/db.py`, `portal/worker.py` | Cola de trabajos en SQLite, ejecución de etapas y publicación transaccional. Los resultados se leen mediante consultas parametrizadas. |
| `portal/web/` | Interfaces web, mapa de zonas de la TLC y vistas de ejecución. |
| `spark/` | PySpark, Spark SQL, GraphFrames y MLlib. Los datos detallados y agregados intermedios residen en HDFS. |
| `cluster/`, `urbanflow.ps1`, `motor-cluster.ps1` | Despliegue, conexión y limpieza del clúster. |
| `dashboard/`, `presentacion/` | Panel y presentación del proyecto original. El portal utiliza la geometría de zonas del dashboard. |

Spark genera salidas por trabajo. El ejecutor carga los agregados y las predicciones en una transacción de SQLite. Si ocurre un error, la publicación anterior sigue disponible. El frontal consume la API mediante HTTP. SQLite vive en el maestro durante la sesión de exposición y el servicio público tiene acceso de lectura. El portal consulta YARN para el estado de aplicaciones y los recursos asignados a los workers. Los registros detallados quedan en el maestro bajo `/var/lib/urbanflow/runs/` y en el journal del servicio `urbanflow-worker`.

Los viajes históricos representan recogidas registradas. El selector de evaluación muestra las fechas para las que se generaron estimaciones y el día siguiente al corte del conjunto de datos.

## Pruebas locales

```powershell
python -m pip install -r portal/requirements.txt
python -m unittest discover -s tests -v
```

Para comprobar que el portal arranca con la base vacía, ejecuta `python -m portal.bootstrap` y después `python -m uvicorn portal.app:app`. Las pruebas construyen su propia base temporal.

## Publicación en GitHub

El repositorio se creó a partir de fuentes seleccionadas. Las llaves de EC2 Instance Connect, el estado de AWS, la base operativa, los logs y los resultados generados están excluidos. La sesión AWS se mantiene en tu entorno. Antes de publicar revisa los archivos incluidos con `git ls-files` y los cambios preparados con `git diff --cached`.

Los scripts heredados están descritos en [LEEME.md](LEEME.md).
