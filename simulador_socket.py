import socket
import threading
import time
import random


def servidor_receptor(host="localhost", puerto=88, total_esperado=10):
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((host, puerto))
    servidor.listen(5)
    print(f"Servidor a la escucha en {host}:{puerto}...")
    recibidos = 0
    try:
        while recibidos < total_esperado:
            conn, _ = servidor.accept()
            datos = conn.recv(1024).decode("utf-8").strip()
            if datos:
                recibidos += 1
                print(f"Mensaje recibido {recibidos}/{total_esperado}: {datos}")
            conn.close()
    finally:
        servidor.close()


def enviar_palabra(palabra, host="localhost", puerto=88):
    cliente_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente_socket.connect((host, puerto))
        palabra = palabra + "\n"
        cliente_socket.sendall(palabra.encode())
        intervalo = random.uniform(0.5, 2)
        time.sleep(intervalo)
    finally:
        cliente_socket.close()


if __name__ == "__main__":
    palabras = ["hola", "mundo", "python", "netcat", "socket"]
    palabras = palabras * 2
    hilo_servidor = threading.Thread(
        target=servidor_receptor, args=("localhost", 88, len(palabras)), daemon=True
    )
    hilo_servidor.start()
    time.sleep(0.5)
    for palabra in palabras:
        enviar_palabra(palabra)
    hilo_servidor.join()
