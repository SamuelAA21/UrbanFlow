# UrbanFlow

Portal de movilidad para explorar recogidas históricas de taxis amarillos en Nueva York y evaluar estimaciones generadas con Spark MLlib. El portal privado muestra el estado de los workers y ejecuta cálculos de corredores por mes en YARN.

## Lanzador gráfico en Windows

Haz doble clic en `UrbanFlow.cmd`. El lanzador permite iniciar sesión en AWS, desplegar uno o seis meses, abrir los paneles, consultar el estado, respaldar SQLite y eliminar los recursos sin escribir comandos. Las acciones de AWS abren una consola visible para mostrar su progreso y sus errores.

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
