from collections import defaultdict
import os
from concurrent.futures import ThreadPoolExecutor
import time

def contar_palabras_en_linea(linea):
    conteo_palabras_local = defaultdict(int)
    palabras = linea.split()
    for palabra in palabras:
        # Limpiar cada palabra de signos de puntuación y convertirla a minúsculas.
        palabra = palabra.strip('.,?!-:;\'"').lower()
        conteo_palabras_local[palabra] += 1
    return conteo_palabras_local

def contar_palabras_en_archivo(nombre_archivo):
    conteo_palabras_global = defaultdict(int)

    # Obtener el número de núcleos de la CPU y utilizar la mitad de ellos
    max_workers = max(1, os.cpu_count() // 2)

    # Abrir el archivo y procesar línea por línea
    with open(nombre_archivo, 'r', encoding='utf-8') as archivo:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Mapear el procesamiento de cada línea a múltiples hilos
            conteos_por_linea = list(executor.map(contar_palabras_en_linea, archivo))

    # Combinar los conteos locales de palabras en un conteo global
    for conteo_palabras_local in conteos_por_linea:
        for palabra, contador in conteo_palabras_local.items():
            conteo_palabras_global[palabra] += contador

    return conteo_palabras_global

nombre_archivo = r"C:\Users\Leoncio Casusol\Downloads\archivo20GB.txt"

print(f"\nAnalizando: {nombre_archivo}")
# Verificar si el archivo existe
if os.path.exists(nombre_archivo):
    # Contar las ocurrencias de todas las palabras en el archivo
    print("Contando palabras....")
    inicio_tiempo = time.time()
    resultado = contar_palabras_en_archivo(nombre_archivo)
    fin_tiempo = time.time()
    tiempo_total_ms = (fin_tiempo - inicio_tiempo) * 1000
    print(f"Se encontraron {len(resultado)} palabras únicas en el archivo.")
    for palabra, frecuencia in resultado.items():
        print(f"Palabra: '{palabra}', Frecuencia: {frecuencia}")
    print(f"Tiempo total de procesamiento: {tiempo_total_ms:.2f} milisegundos")
else:
    print(f"El archivo '{nombre_archivo}' no existe.")
