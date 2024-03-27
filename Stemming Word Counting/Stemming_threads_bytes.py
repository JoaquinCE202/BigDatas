import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

# Inicialización del stemmer de NLTK
stemmer = PorterStemmer()

def procesar_bloque_de_lineas(bloque_lineas):
    """Procesa un bloque de líneas realizando el stemming y contando las palabras manualmente."""
    contador_local = {}
    palabras = word_tokenize(bloque_lineas.lower())
    raices = [stemmer.stem(palabra) for palabra in palabras]
    for raiz in raices:
        if raiz in contador_local:
            contador_local[raiz] += 1
        else:
            contador_local[raiz] = 1
    return contador_local

def dividir_en_bloques_por_tamaño(archivo, tamaño_bloque_bytes=50*1024*1024):
    """Divide el archivo en bloques basados en un tamaño aproximado en bytes, 
    sin cortar palabras a la mitad."""
    while True:
        inicio_bloque = archivo.tell()  # Guarda la posición actual del archivo
        bloque = archivo.read(tamaño_bloque_bytes)  # Lee el tamaño de bloque deseado
        if not bloque:  # Si no se lee nada, el archivo ha terminado
            break
        
        fin_bloque = archivo.tell()  # Guarda la posición después de leer
        ultimo_caracter = bloque[-1]
        
        # Si el último carácter no es un delimitador de palabra, lee hasta el siguiente delimitador
        if not ultimo_caracter.isspace():
            resto_palabra = archivo.readline()  # Leer hasta el final de la palabra actual
            bloque += resto_palabra
        
        yield bloque

        # Si el archivo se leyó hasta el final en la llamada readline(), termina el ciclo
        if archivo.tell() == fin_bloque:
            break

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

def leer_y_contar_concurrente(ruta_archivo, num_hilos=6):
    start_time = time.time()

    with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
        bloques = list(dividir_en_bloques_por_tamaño(archivo))

    resultados = []
    with ThreadPoolExecutor(max_workers=num_hilos) as executor:
        futuros = [executor.submit(procesar_bloque_de_lineas, bloque) for bloque in bloques]
        
        for futuro in as_completed(futuros):
            resultados.append(futuro.result())
    
    frecuencias_global = combinar_frecuencias(resultados)
    
    end_time = time.time()

    for palabra, frecuencia in sorted(frecuencias_global.items(), key=lambda item: item[1], reverse=True):
        print(f"Palabra: {palabra}, Frecuencia: {frecuencia}")

    print(f"\nEl proceso tomó {end_time - start_time} segundos.")

# Ruta del archivo a leer
ruta_archivo = r"D:\archivo20GB.txt"
leer_y_contar_concurrente(ruta_archivo)
