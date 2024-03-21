import nltk
from nltk.stem import PorterStemmer
from collections import Counter
import time  # Importamos el módulo time

# Descargar los datos necesarios de NLTK, solo es necesario hacerlo una vez
# nltk.download('punkt')

# Inicializamos el stemmer
stemmer = PorterStemmer()

def leer_y_contar(ruta_archivo):
    frecuencias = Counter()
    start_time = time.time()  # Guardamos el tiempo de inicio
    
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
            # Leer el archivo línea por línea
            for linea in archivo:
                palabra = linea.strip().lower()  # Eliminamos espacios y convertimos a minúsculas
                # Aplicamos el proceso de stemming a cada palabra
                raiz_palabra = stemmer.stem(palabra)
                # Contabilizamos la raíz de la palabra
                frecuencias[raiz_palabra] += 1
    except Exception as e:
        print(f"Se produjo un error al leer el archivo: {e}")
        return
    
    end_time = time.time()  # Guardamos el tiempo de fin
    total_time = end_time - start_time  # Calculamos la duración total
    
    # Imprimir las frecuencias de las palabras
    for palabra, frecuencia in frecuencias.items():
        print(f"Palabra: {palabra}, Frecuencia: {frecuencia}")
    
    # Imprimir la duración total del proceso
    print(f"\nEl proceso tomó {total_time} segundos.")

# Ruta del archivo a leer
ruta_archivo = r"C:\Users\Leoncio Casusol\Downloads\archivo_grande.txt"

leer_y_contar(ruta_archivo)
