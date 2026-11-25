# Práctica de Apache Spark Structured Streaming

Este documento recoge el desarrollo completo de la práctica de procesamiento de flujos de datos en tiempo real con Apache Spark Structured Streaming y Netcat. Toda la redacción evita el uso de paréntesis para hacer énfasis o aclaraciones.

---

## 1. Preparación de la fuente de datos con Netcat y Python

Para simular el envío continuo de texto a través de una conexión de red TCP se utiliza la utilidad `ncat` o `netcat`. En modo servidor con escucha persistente se ejecuta el siguiente comando en una terminal:

```bash
nc -lk -p 9999
```

La bandera `-l` indica que el programa debe quedar a la escucha de conexiones entrantes, la bandera `-k` fuerza a mantener el socket abierto tras cada transmisión y `-p 9999` define el puerto local utilizado.

Además, el archivo `simulador_socket.py` permite automatizar el envío de palabras hacia un socket local con pausas aleatorias de entre medio segundo y dos segundos.

---

## 2. Caso 1: Hola Spark Streaming en `Streaming1.ipynb`

El cuaderno `Streaming1.ipynb` implementa un contador de palabras en tiempo real dividido en cuatro etapas fundamentales:

1. **Inicialización de la sesión**: Se crea un objeto `SparkSession` con `.master("local[2]")` para garantizar un mínimo de dos hilos de ejecución, destinando un hilo a la recepción de datos del socket y otro al procesamiento de las transformaciones.
2. **Lectura del flujo**: Mediante `spark.readStream.format("socket")` se establece la conexión hacia `localhost` en el puerto `9999`, generando un DataFrame de flujo llamado `lineasDF` donde cada línea recibida ocupa la columna `value`.
3. **Transformación y agregación**: Se divide cada línea por espacios con `split` y se transforma cada elemento del arreglo resultante en una fila independiente con `explode`. Luego se agrupa por la columna `palabra` y se aplica `count` para obtener el total acumulado en `cantidadDF`.
4. **Escritura y espera**: Con `writeStream.format("console")` y modo de salida `complete` se inicia la consulta mediante `start` y se mantiene activa con `awaitTermination`.

---

## 3. Ejercicio 1: Comparación de los modos de salida en `Streaming2.ipynb`

En este ejercicio se duplica el flujo base en `Streaming2.ipynb` y se envían de forma secuencial las dos frases propuestas en la práctica:

* Primera frase enviada: `Esta es la primera frase`
* Segunda frase enviada: `Enviamos segunda linea primera`

### Comportamiento con el modo `complete`

Cuando el flujo utiliza `outputMode` configurado en `complete`, Spark conserva en el estado interno todas las claves vistas desde el arranque de la aplicación y reescribe la tabla entera en cada micro-lote:

* **Lote 1**: Tras recibir la primera frase, la consola imprime las cinco palabras `Esta`, `es`, `la`, `primera` y `frase`, todas con un conteo igual a `1`.
* **Lote 2**: Tras recibir la segunda frase, la consola vuelve a imprimir las ocho palabras registradas hasta ese momento. La palabra `primera` aparece con un conteo de `2`, mientras que `Esta`, `es`, `la`, `frase`, `Enviamos`, `segunda` y `linea` aparecen con un conteo de `1`.

### Comportamiento con el modo `update`

Al detener la consulta, cambiar `outputMode` al valor `update`, reiniciar la ejecución y volver a enviar las dos mismas frases, el resultado cambia de forma notable respecto al modo anterior:

* **Lote 1**: Como todas las palabras de la primera frase son nuevas en el estado, se muestran las cinco filas `Esta`, `es`, `la`, `primera` y `frase` con valor `1`.
* **Lote 2**: A diferencia del modo completo, aquí únicamente se emiten por consola las filas que sufrieron alguna modificación o inserción durante este segundo micro-lote. Por lo tanto, solo aparecen cuatro filas: `primera` con su nuevo valor actualizado a `2`, junto con las nuevas palabras `Enviamos`, `segunda` y `linea` con valor `1`. Las palabras `Esta`, `es`, `la` y `frase` no se muestran en este lote porque su conteo permaneció intacto.

### Comportamiento con el modo `append`

Al cambiar `outputMode` al valor `append` e intentar iniciar el flujo, Spark no llega a procesar las frases y detiene la ejecución lanzando de inmediato un error de tipo `AnalysisException`:

```text
AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
```

**Explicación técnica del fallo**: El modo de adición o `append` está diseñado exclusivamente para emitir filas cuyo resultado ya es definitivo y jamás va a modificarse en lotes futuros. En una agregación con estado como `groupBy("palabra").count()` sin una marca de agua temporal o watermark, Spark nunca puede dar por cerrado el conteo de ninguna palabra, puesto que esa misma palabra podría volver a llegar por el socket en cualquier momento posterior. Al no poder garantizar la inmutabilidad de las filas emitidas, el motor de Spark SQL bloquea la consulta.

---

## 4. Ejercicio 2: Uso de Triggers en `Streaming2.ipynb`

En la segunda parte de `Streaming2.ipynb` se modifica la escritura del flujo para incorporar un disparador temporal de un minuto:

```python
wordCountQuery = cantidadDF.writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="1 minute") \
    .start()
```

### Qué se observa al enviar frases espaciadas por menos de un minuto

1. **Sin disparador explícito**: En el comportamiento por defecto, Spark ejecuta un nuevo micro-lote tan pronto como detecta la llegada de datos por el socket y finaliza el lote previo. Por ello, al pulsar Enter en Netcat tras cada frase, la consola de Spark muestra un lote nuevo casi al instante.
2. **Con disparador de un minuto**: Al configurar `processingTime="1 minute"`, el motor de Spark SQL acumula todas las líneas que llegan al socket durante ese intervalo de sesenta segundos. Cuando se envían las frases `Esta es la primera frase` y `Enviamos segunda linea primera` con pocos segundos de separación entre ambas, la consola permanece en espera y no imprime ningún lote intermedio tras la primera frase. Una vez que se cumple el minuto del reloj del disparador, Spark agrupa todas las líneas recolectadas en ese periodo y las procesa juntas dentro de un único micro-lote, mostrando directamente el resultado combinado con la palabra `primera` contabilizada dos veces.
