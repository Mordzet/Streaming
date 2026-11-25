import os
import sys
import socket
import threading
import time
import random
from contextlib import redirect_stdout
import io
import streamlit as st
import pandas as pd

jdk_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.10002-hotspot"
if os.path.exists(jdk_path):
    os.environ["JAVA_HOME"] = jdk_path
    os.environ["PATH"] = jdk_path + r"\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

st.set_page_config(
    page_title="Práctica de Apache Spark Structured Streaming",
    layout="wide",
)


@st.cache_resource
def obtener_spark():
    spark = (
        SparkSession.builder.appName("Streaming IABD WordCount")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def obtener_puerto_libre():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def servidor_envio_frases(puerto, frases, espera_inicial=1.5, pausa_entre_frases=2.5):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("localhost", puerto))
    servidor.listen(1)
    conn, _ = servidor.accept()
    time.sleep(espera_inicial)
    for frase in frases:
        conn.sendall((frase + "\n").encode("utf-8"))
        time.sleep(pausa_entre_frases)
    time.sleep(2.5)
    conn.close()
    servidor.close()


def ejecutar_flujo_memoria(spark, frases, modo_salida="complete", trigger_seg=None):
    puerto = obtener_puerto_libre()
    nombre_tabla = f"tabla_streaming_{int(time.time() * 1000)}_{random.randint(100, 999)}"
    pausa = 1.0 if trigger_seg else 2.5
    hilo = threading.Thread(
        target=servidor_envio_frases,
        args=(puerto, frases, 1.5, pausa),
        daemon=True,
    )
    hilo.start()

    lineasDF = (
        spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", str(puerto))
        .load()
    )
    palabrasDF = lineasDF.select(explode(split(lineasDF.value, " ")).alias("palabra"))
    cantidadDF = palabrasDF.groupBy("palabra").count()

    lotes_capturados = []

    def procesar_lote(df_lote, id_lote):
        filas = df_lote.collect()
        datos = [{"palabra": r["palabra"], "count": r["count"]} for r in filas]
        lotes_capturados.append((id_lote, datos))

    escritor = cantidadDF.writeStream.outputMode(modo_salida).foreachBatch(procesar_lote)
    if trigger_seg:
        escritor = escritor.trigger(processingTime=f"{trigger_seg} seconds")

    consulta = escritor.start()
    tiempo_espera = max(7.0, len(frases) * pausa + 4.0)
    if trigger_seg:
        tiempo_espera = max(tiempo_espera, trigger_seg + 4.0)
    time.sleep(tiempo_espera)
    consulta.stop()
    hilo.join(timeout=2)
    return lotes_capturados


st.title("Práctica de Apache Spark Structured Streaming")
st.markdown(
    "Panel interactivo para ejecutar en vivo cada sección de la práctica de **Streaming en Spark** sin usar paréntesis para hacer énfasis."
)

pestana1, pestana2, pestana3, pestana4 = st.tabs(
    [
        "1. Herramienta Netcat y Socket en Python",
        "2. Caso 1: Hola Spark Streaming",
        "3. Ejercicio 1: Modos de Salida",
        "4. Ejercicio 2: Triggers",
    ]
)

with pestana1:
    st.header("Simulador de envío continuo por Socket TCP")
    st.markdown(
        "En esta sección se prueba el envío y recepción de palabras a través de un socket TCP local tal como se describe en el apartado de `ncat`."
    )
    palabras_input = st.text_input(
        "Palabras base a enviar por el socket separadas por coma:",
        value="hola, mundo, python, netcat, socket",
    )
    if st.button("Ejecutar simulador de Socket TCP", key="btn_socket"):
        lista_palabras = [p.strip() for p in palabras_input.split(",") if p.strip()] * 2
        puerto_prueba = obtener_puerto_libre()
        registros = []
        barra = st.progress(0)
        contenedor_log = st.empty()

        def receptor():
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("localhost", puerto_prueba))
            srv.listen(5)
            for _ in range(len(lista_palabras)):
                conn, _ = srv.accept()
                dato = conn.recv(1024).decode("utf-8").strip()
                registros.append(dato)
                conn.close()
            srv.close()

        t_srv = threading.Thread(target=receptor, daemon=True)
        t_srv.start()
        time.sleep(0.3)

        for idx, pal in enumerate(lista_palabras, start=1):
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect(("localhost", puerto_prueba))
            cli.sendall((pal + "\n").encode("utf-8"))
            cli.close()
            time.sleep(0.3)
            barra.progress(idx / len(lista_palabras))
            contenedor_log.code(
                "\n".join(
                    [f"Mensaje recibido {i+1}/{len(lista_palabras)}: {m}" for i, m in enumerate(registros)]
                )
            )
        t_srv.join(timeout=2)
        st.success(f"Se enviaron y recibieron {len(registros)} palabras por el socket local en el puerto {puerto_prueba}.")

with pestana2:
    st.header("Caso 1: Hola Spark Streaming")
    st.markdown(
        "Ejecución en tiempo real del contador de palabras en Spark Structured Streaming con `SparkSession` a dos hilos locales y salida en modo `complete`."
    )
    f1_caso1 = st.text_input("Primera frase a enviar:", value="Esta es la primera frase", key="c1_f1")
    f2_caso1 = st.text_input("Segunda frase a enviar:", value="Enviamos segunda linea primera", key="c1_f2")

    if st.button("Ejecutar Caso 1 en PySpark", key="btn_caso1"):
        with st.spinner("Iniciando SparkSession y procesando flujo por socket..."):
            spark = obtener_spark()
            lotes = ejecutar_flujo_memoria(spark, [f1_caso1, f2_caso1], modo_salida="complete")
        st.success("Flujo de Spark Streaming ejecutado correctamente.")
        for id_lote, filas in lotes:
            st.subheader(f"Micro-lote {id_lote}")
            if filas:
                st.dataframe(pd.DataFrame(filas), use_container_width=True)
            else:
                st.info("Lote inicial vacío sin palabras recibidas aún.")

with pestana3:
    st.header("Ejercicio 1: Compara los modos de salida")
    st.markdown(
        "Prueba comparativa enviando las frases `Esta es la primera frase` y `Enviamos segunda linea primera` bajo los tres modos de salida de Structured Streaming."
    )
    modo_elegido = st.radio(
        "Selecciona el modo de salida a ejecutar en vivo:",
        options=["complete", "update", "append"],
        horizontal=True,
    )

    if st.button(f"Ejecutar prueba en modo {modo_elegido}", key="btn_ej1"):
        spark = obtener_spark()
        frases_ej1 = ["Esta es la primera frase", "Enviamos segunda linea primera"]
        if modo_elegido == "append":
            puerto_err = obtener_puerto_libre()
            lineas_err = (
                spark.readStream.format("socket")
                .option("host", "localhost")
                .option("port", str(puerto_err))
                .load()
            )
            palabras_err = lineas_err.select(explode(split(lineas_err.value, " ")).alias("palabra"))
            cantidad_err = palabras_err.groupBy("palabra").count()
            try:
                q_err = cantidad_err.writeStream.format("console").outputMode("append").start()
                q_err.stop()
            except Exception as exc:
                st.error("Spark lanzó la excepción esperada al usar el modo append en una agregación sin marca de agua:")
                st.code(f"{type(exc).__name__}: {str(exc).splitlines()[0]}")
                st.markdown(
                    "**Explicación**: El modo `append` exige que las filas emitidas sean definitivas y no cambien jamás, pero en un conteo continuo sin watermark cualquier palabra puede repetirse en lotes posteriores y alterar su recuento acumulado."
                )
        else:
            with st.spinner(f"Procesando micro-lotes con outputMode={modo_elegido}..."):
                lotes_ej1 = ejecutar_flujo_memoria(spark, frases_ej1, modo_salida=modo_elegido)
            for id_lote, filas in lotes_ej1:
                st.subheader(f"Micro-lote {id_lote} en modo {modo_elegido}")
                if filas:
                    st.dataframe(pd.DataFrame(filas), use_container_width=True)
                else:
                    st.info("Lote sin registros.")
            if modo_elegido == "complete":
                st.markdown(
                    "**Observación**: En el modo `complete`, cada micro-lote imprime la tabla entera con las ocho palabras acumuladas desde el comienzo de la consulta."
                )
            elif modo_elegido == "update":
                st.markdown(
                    "**Observación**: En el modo `update`, el segundo micro-lote muestra únicamente las cuatro filas que cambiaron o se insertaron en ese lote: `primera` con valor 2 y `Enviamos`, `segunda` y `linea` con valor 1. Las demás palabras de la primera frase se omiten porque no cambiaron."
                )

with pestana4:
    st.header("Ejercicio 2: Triggers")
    st.markdown(
        "Al configurar un intervalo de disparo mediante `.trigger(processingTime=...)`, Spark recolecta todas las frases enviadas dentro de esa ventana de tiempo y las procesa juntas en un solo micro-lote."
    )
    seg_trigger = st.slider(
        "Intervalo del disparador en segundos para la demostración en vivo:",
        min_value=4,
        max_value=60,
        value=5,
    )
    if st.button("Ejecutar flujo con Trigger", key="btn_trigger"):
        spark = obtener_spark()
        frases_ej2 = ["Esta es la primera frase", "Enviamos segunda linea primera"]
        with st.spinner(
            f"Enviando ambas frases espaciadas por 1 segundo y esperando el disparador de {seg_trigger} segundos..."
        ):
            lotes_trig = ejecutar_flujo_memoria(
                spark, frases_ej2, modo_salida="complete", trigger_seg=seg_trigger
            )
        st.success(f"Disparador de {seg_trigger} segundos completado.")
        for id_lote, filas in lotes_trig:
            st.subheader(f"Micro-lote {id_lote}")
            if filas:
                st.dataframe(pd.DataFrame(filas), use_container_width=True)
            else:
                st.info("Lote inicial vacío.")
        st.markdown(
            "**Observación**: Aunque se enviaron dos frases separadas por un segundo, el disparador agrupó ambas entradas dentro de la misma ventana temporal y emitió directamente el resultado combinado en un solo micro-lote con la palabra `primera` contabilizada 2 veces."
        )
