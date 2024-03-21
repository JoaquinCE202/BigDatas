import os
from collections import Counter
import threading
import time

def limpiar_y_contar(linea, contador_compartido):
    # Eliminar puntuación y convertir a minúscula
    palabras = [palabra.strip('.,?!-:;"\'').lower() for palabra in linea.split()]
    # Actualizar el contador compartido con las palabras de la línea
    with threading.Lock():
        contador_compartido.update(palabras)

def procesar_archivo_con_hilos(archivo, num_hilos=4):
    # Contador global para todas las palabras
    contador_global = Counter()

    with open(archivo, 'r', encoding='utf-8') as f:
        lineas = f.readlines()
    
    # Dividir las líneas entre los hilos disponibles
    tamanio_parte = len(lineas) // num_hilos + 1
    hilos = []
    
    for i in range(0, len(lineas), tamanio_parte):
        segmento = lineas[i:i+tamanio_parte]
        hilo = threading.Thread(target=limpiar_y_contar, args=(segmento, contador_global))
        hilos.append(hilo)
        hilo.start()
    
    for hilo in hilos:
        hilo.join()

    return contador_global

if __name__ == "__main__":
    archivo_a_procesar = r"C:\Users\Leoncio Casusol\Downloads\archivo20GB.txt"
    if os.path.exists(archivo_a_procesar):
        print(f"Comenzando el procesamiento de: {archivo_a_procesar}")
        tiempo_inicio = time.time()
        resultado = procesar_archivo_con_hilos(archivo_a_procesar)
        tiempo_fin = time.time()
        for palabra, cantidad in resultado.most_common(10):  # Mostrar las 10 palabras más comunes
            print(f"{palabra}: {cantidad}")
        print(f"Duración: {(tiempo_fin - tiempo_inicio):.2f} segundos.")
    else:
        print(f"Archivo no encontrado: {archivo_a_procesar}")
