import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

stemmer = PorterStemmer()

def procesar_bloque_de_lineas(bloque_lineas):
    """Procesa un bloque de líneas realizando el stemming y contando las palabras manualmente."""
    contador_local = {}
    for linea in bloque_lineas:
        palabras = word_tokenize(linea.lower())
        raices = [stemmer.stem(palabra) for palabra in palabras]
        for raiz in raices:
            if raiz in contador_local:
                contador_local[raiz] += 1
            else:
                contador_local[raiz] = 1
    return contador_local

def dividir_en_bloques(archivo, tamaño_bloque=100):
    """Divide el archivo en bloques de un cierto número de líneas."""
    bloque = []
    for linea in archivo:
        bloque.append(linea)
        if len(bloque) == tamaño_bloque:
            yield bloque
            bloque = []
    if bloque:
        yield bloque

def combinar_frecuencias(frecuencias_list):
    """Combina las frecuencias de palabras de múltiples diccionarios en uno solo."""
    frecuencias_global = {}
    for frecuencias in frecuencias_list:
        for palabra, frecuencia in frecuencias.items():
            if palabra in frecuencias_global:
                frecuencias_global[palabra] += frecuencia
            else:
                frecuencias_global[palabra] = frecuencia
    return frecuencias_global

def leer_y_contar_concurrente(ruta_archivo, num_hilos=10):
    start_time = time.time()

    with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
        bloques = list(dividir_en_bloques(archivo))

    resultados = []
    # Iniciar un pool de hilos para procesar los bloques de texto en paralelo.
    with ThreadPoolExecutor(max_workers=num_hilos) as executor:
        # Programar las tareas de procesamiento para cada bloque de texto.
        # Cada tarea es programada para ejecución y devuelve un objeto Future.
        futuros = [executor.submit(procesar_bloque_de_lineas, bloque) for bloque in bloques]
        
        # Iterar sobre los futuros a medida que completan su ejecución.
        # as_completed devuelve los futuros en el orden en que completan, permitiendo procesar los resultados de inmediato.
        for futuro in as_completed(futuros):
            # Obtener el resultado de la tarea completada y agregarlo a la lista de resultados.
            resultados.append(futuro.result())

    frecuencias_global = combinar_frecuencias(resultados)
    
    end_time = time.time()

    # Imprimir las frecuencias de las palabras
    for palabra, frecuencia in sorted(frecuencias_global.items(), key=lambda item: item[1], reverse=True):
        print(f"Palabra: {palabra}, Frecuencia: {frecuencia}")

    # Imprimir la duración total del proceso
    print(f"\nEl proceso tomó {end_time - start_time} segundos.")

# Ruta del archivo a leer
ruta_archivo = r"C:\Users\alumno-b303\Documents\test05.txt"
leer_y_contar_concurrente(ruta_archivo)
