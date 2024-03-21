import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import time
import nltk
from nltk.stem import PorterStemmer



def ampliar_archivo_hasta(ruta_archivo, tamano_final_mb):
    tamano_final_bytes = tamano_final_mb * 1024 * 1024  # Convertir MB a Bytes
    
    primera_linea = ''
    with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
        primera_linea = archivo.readline()  # Leer la primera línea
        
    if primera_linea:
        with open(ruta_archivo, 'a', encoding='utf-8') as archivo:
            while os.path.getsize(ruta_archivo) < tamano_final_bytes:
                archivo.write(primera_linea)
                # Opcionalmente, puede verificar el tamaño del archivo cada N escrituras para reducir el número de llamadas a os.path.getsize()
    else:
        print("El archivo está vacío.")

# Ruta del archivo a modificar
ruta_archivo = r"C:\Users\alumno-b303\Downloads\archivo20GB.txt"

# Llamar a la función para ampliar el archivo hasta 1.5 GB
ampliar_archivo_hasta(ruta_archivo, 1500)  # 1.5 GB en MB
