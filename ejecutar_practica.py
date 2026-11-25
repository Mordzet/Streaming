import os
import sys
import socket
import threading
import time

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.21.10002-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


def servidor_socket(puerto, frases, espera_inicial=2, pausa_entre_frases=3):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("localhost", puerto))
    servidor.listen(1)
    conn, _ = servidor.accept()
    time.sleep(espera_inicial)
    for frase in frases:
        print(f">> Enviando por socket en puerto {puerto}: {frase}")
        conn.sendall((frase + "\n").encode("utf-8"))
        time.sleep(pausa_entre_frases)
    time.sleep(3)
    conn.close()
    servidor.close()


def crear_flujo_conteo(spark, puerto):
    lineasDF = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", str(puerto)) \
        .load()
    palabrasDF = lineasDF.select(explode(split(lineasDF.value, " ")).alias("palabra"))
    return palabrasDF.groupBy("palabra").count()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Streaming IABD WordCount") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    frases_prueba = [
        "Esta es la primera frase",
        "Enviamos segunda linea primera"
    ]

    print("\n=======================================================")
    print("1. CASO 1 Y EJERCICIO 1A: MODO DE SALIDA COMPLETE")
    print("=======================================================")
    hilo1 = threading.Thread(target=servidor_socket, args=(9999, frases_prueba, 2, 3))
    hilo1.start()
    cantidadDF1 = crear_flujo_conteo(spark, 9999)
    consulta1 = cantidadDF1.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()
    time.sleep(9)
    consulta1.stop()
    hilo1.join()

    print("\n=======================================================")
    print("2. EJERCICIO 1B: MODO DE SALIDA UPDATE")
    print("=======================================================")
    hilo2 = threading.Thread(target=servidor_socket, args=(9998, frases_prueba, 2, 3))
    hilo2.start()
    cantidadDF2 = crear_flujo_conteo(spark, 9998)
    consulta2 = cantidadDF2.writeStream \
        .format("console") \
        .outputMode("update") \
        .start()
    time.sleep(9)
    consulta2.stop()
    hilo2.join()

    print("\n=======================================================")
    print("3. EJERCICIO 1C: MODO DE SALIDA APPEND")
    print("=======================================================")
    try:
        consulta3 = cantidadDF2.writeStream \
            .format("console") \
            .outputMode("append") \
            .start()
    except Exception as error:
        print("Excepcion capturada en modo append:")
        print(f"{type(error).__name__}: {str(error).splitlines()[0]}")

    print("\n=======================================================")
    print("4. EJERCICIO 2: DISPARADOR CON TRIGGER PROCESSINGTIME")
    print("=======================================================")
    hilo4 = threading.Thread(target=servidor_socket, args=(9997, frases_prueba, 1, 1))
    hilo4.start()
    cantidadDF4 = crear_flujo_conteo(spark, 9997)
    consulta4 = cantidadDF4.writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="5 seconds") \
        .start()
    time.sleep(8)
    consulta4.stop()
    hilo4.join()

    spark.stop()
    print("\nEjecucion completa de la practica finalizada con exito.")
