import os
from multiprocessing import Pool, cpu_count
import time

def process_chunk(chunk_data):
    """
    Procesa un chunk específico del archivo, construyendo un diccionario con palabras como claves
    y una lista de archivos en los que aparecen como valores.
    """
    file_path, start, end = chunk_data
    word_dict = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        # Mueve el cursor al inicio del chunk para comenzar la lectura
        file.seek(start)
        # Lee solo la porción del archivo asignada a este chunk
        text = file.read(end - start)
        for word in text.split():
            word = word.lower()
            if word in word_dict:
                if file_path not in word_dict[word]:
                    word_dict[word].append(file_path)
            else:
                word_dict[word] = [file_path]
    return word_dict

def combine_dicts(dicts):
    """
    Combina los diccionarios de palabras de todos los chunks en un solo diccionario.
    Usa conjuntos para asegurar que cada archivo se liste una sola vez por palabra.
    """
    combined = {}
    for d in dicts:
        for k, v in d.items():
            if k in combined:
                # Usa union de conjuntos para evitar duplicados
                combined[k] = combined[k].union(set(v))
            else:
                combined[k] = set(v)
    # Convierte los conjuntos a listas para el resultado final
    return {key: list(value) for key, value in combined.items()}

def chunkify(file_path, num_chunks):
    """
    Divide un archivo en chunks para procesamiento paralelo, basado en el tamaño del archivo.
    """
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_chunks
    return [(file_path, i * chunk_size, (i + 1) * chunk_size) for i in range(num_chunks)]

def create_inverted_index(file_paths, num_processes=None):
    """
    Crea un índice invertido de palabras en archivos, utilizando procesamiento paralelo.
    """
    if num_processes is None:
        num_processes = cpu_count()
    
    pool = Pool(processes=num_processes)
    chunk_data_list = []
    # Divide cada archivo en chunks y los agrega a una lista para procesar
    for file_path in file_paths:
        chunks = chunkify(file_path, num_processes)
        chunk_data_list.extend(chunks)
    
    # Procesa cada chunk en paralelo y recoge los resultados
    dicts = pool.map(process_chunk, chunk_data_list)
    inverted_index = combine_dicts(dicts)
    return inverted_index

def main():
    base_path = "D:/"
    file_paths = [f"{base_path}archivo{i}.txt" for i in range(1, 11)]  # Ajustado para 10 archivos

    start_time = time.time()
    inverted_index = create_inverted_index(file_paths)
    end_time = time.time()
    print(f"Índice invertido creado en {end_time - start_time} segundos.")

    word_to_search = input("Ingrese la palabra a buscar: ").lower()

    start_time = time.time()
    found_files = inverted_index.get(word_to_search, [])
    end_time = time.time()
    if found_files:
        print(f"La palabra '{word_to_search}' se encontró en los siguientes documentos: {found_files}")
    else:
        # Mensaje si no se encuentra la palabra en ningún documento
        print(f"La palabra '{word_to_search}' no se encontró en ninguno de los documentos.")
    print(f"Búsqueda completada en {end_time - start_time} segundos.")

if __name__ == '__main__':
    main()
