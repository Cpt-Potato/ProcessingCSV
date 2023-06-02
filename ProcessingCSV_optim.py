import csv
import os
from collections import defaultdict

cdr_directory = "Синтетические данные (CSV)"
prefixes_file = "Префиксы телефонных номеров (CSV)/PREFIXES.TXT"
output_directory = "Выходные данные (CSV)"
volumes_file = "Выходные данные (CSV)/VOLUMES.TXT"

# Шаг 1: Получить список файлов CDR
cdr_files = os.listdir(cdr_directory)

# Шаг 2: Загрузить содержимое файла PREFIXES.TXT и предварительно отсортировать префиксы
prefixes = {}
with open(prefixes_file, "r") as file:
    reader = csv.reader(file)
    prefixes = {row[1]: row[0] for row in sorted(reader, key=lambda x: len(x[1]), reverse=True) if len(row) >= 2}

# Шаг 3: Создать пустой словарь для агрегированных данных о длительности соединений
aggregated_data = defaultdict(int)

# Шаг 4: Обработка каждого файла CDR
for cdr_file in cdr_files:
    cdr_path = os.path.join(cdr_directory, cdr_file)
    output_path = os.path.join(output_directory, cdr_file)

    with open(cdr_path, "r") as cdr_input, open(output_path, "w") as cdr_output:
        reader = csv.reader(cdr_input)
        writer = csv.writer(cdr_output)

        for row in reader:
            if len(row) >= 25:
                msisdn = row[5]
                dialed = row[6]
                msisdn_prefix = next((prefixes[prefix] for prefix in prefixes if msisdn.startswith(prefix)), None)
                dialed_prefix = next((prefixes[prefix] for prefix in prefixes if dialed.startswith(prefix)), None)

                # Обновление полей записи с префиксными зонами
                row[9] = msisdn_prefix or "Unknown"
                row[10] = dialed_prefix or "Unknown"

                # Запись обновленной строки в выходной файл CDR
                writer.writerow(row)

                # Обновление агрегированных данных о длительности соединений
                duration = int(row[8])
                if msisdn_prefix and dialed_prefix:
                    key = (msisdn_prefix, dialed_prefix)
                    aggregated_data[key] += duration

# Шаг 5: Запись агрегированных данных о длительности соединений в файл VOLUMES.TXT
with open(volumes_file, "w") as volumes_output:
    writer = csv.writer(volumes_output, delimiter=',', lineterminator='\n')
    writer.writerows(aggregated_data.keys())
