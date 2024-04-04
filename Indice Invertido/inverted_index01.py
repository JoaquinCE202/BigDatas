import os
from multiprocessing import Pool, Manager, freeze_support
import time  # Importamos el módulo time

def index_file(args):
    file_path, word_dict = args
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            for word in line.strip().split():
                word = word.lower()
                if word in word_dict:
                    if file_path not in word_dict[word]:
                        word_dict[word].append(file_path)
                else:
                    word_dict[word] = [file_path]

def create_inverted_index(file_paths, num_processes=12):
    manager = Manager()
    word_dict = manager.dict()
    with Pool(processes=num_processes) as pool:
        pool.map(index_file, [(file_path, word_dict) for file_path in file_paths])
    return dict(word_dict)

def search(word, index):
    return index.get(word.lower(), [])

def main():
    base_path = "D:/"
    file_paths = [f"{base_path}archivo{i}.txt" for i in range(1, 6)]

    start_time = time.time()  # Iniciamos el conteo de tiempo para la indexación
    inverted_index = create_inverted_index(file_paths, num_processes=4)
    end_time = time.time()  # Finalizamos el conteo de tiempo para la indexación
    print(f"Índice invertido creado en {end_time - start_time} segundos.")

    word_to_search = input("Ingrese la palabra a buscar: ")

    start_time = time.time()  # Iniciamos el conteo de tiempo para la búsqueda
    found_files = search(word_to_search, inverted_index)
    end_time = time.time()  # Finalizamos el conteo de tiempo para la búsqueda
    print(f"La palabra '{word_to_search}' se encontró en los siguientes documentos: {found_files}")
    print(f"Búsqueda completada en {end_time - start_time} segundos.")

if __name__ == '__main__':
    freeze_support()  # Agregar esto es importante para la compatibilidad de Windows.
    main()
