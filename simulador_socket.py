import socket
import time
import random


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
    for palabra in palabras:
        enviar_palabra(palabra)
