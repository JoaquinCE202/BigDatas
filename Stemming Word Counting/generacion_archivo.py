import os

# Tamaño objetivo
target_size_gb = 1
target_size_bytes = target_size_gb * (1024 ** 3)  # 1GB en bytes

# Tamaño de bloque de escritura
block_size_bytes = 1024 ** 2  # 1MB

# Contenidos para cada archivo
contents = [
    "manzana platano fresa kiwi mango pina sandia uva naranja papaya\n",
    "brasil canada egipto francia india japon mexico noruega rusia tailandia\n",
    "leon elefante canguro rinoceronte jirafa koala panda tiburon aguila camello\n",
    "ingles espanol mandarin frances aleman japones ruso arabe portugues italiano\n",
    "guitarra piano violin bateria flauta saxofon trompeta clarinete arpa cello\n",
    "ford toyota bmw mercedes-benz honda audi tesla volkswagen porsche nissan\n",
    "don quijote de la mancha el principito cien anos de soledad la odisea orgullo y prejuicio moby dick harry potter el senor de los anillos\n",
    "el padrino forrest gump la lista de schindler la comunidad del anillo titanic matrix inception el club de la lucha intocable gladiator\n"
]

for i, content in enumerate(contents, start=3):  # Empieza con el archivo3.txt
    target_file_path = r"D:\archivo{}.txt".format(i)
    repetitions_needed = target_size_bytes // len(content.encode('utf-8'))
    
    with open(target_file_path, "w", encoding='utf-8') as file:
        for _ in range(repetitions_needed):
            file.write(content)
    
    print(f"Archivo {target_file_path} de 1GB creado exitosamente.")
