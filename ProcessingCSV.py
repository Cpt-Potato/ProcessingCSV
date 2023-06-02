import csv
import os

cdr_directory = "Синтетические данные (CSV)"
prefixes_file = "Префиксы телефонных номеров (CSV)/PREFIXES.TXT"
output_directory = "Выходные данные (CSV)"
volumes_file = "Выходные данные (CSV)/VOLUMES.TXT"

# Шаг 1: Получить список файлов CDR
cdr_files = os.listdir(cdr_directory)

# Шаг 2: Загрузить содержимое файла PREFIXES.TXT и отсортировать
prefixes = {}
with open(prefixes_file, "r") as file:
    reader = csv.reader(file)
    for row in reader:
        if len(row) >= 2:
            prefix_zone = row[0]
            prefix_numbers = row[1].split(",")
            for number in prefix_numbers:
                prefixes[number] = prefix_zone
prefixes_sorted = sorted(prefixes.keys(), key=len, reverse=True)

# Шаг 3: Создать пустой список для агрегированных данных о длительности соединений
aggregated_data = []

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
                msisdn_prefix = None
                dialed_prefix = None

                # Поиск префиксной зоны для номера MSISDN
                for prefix in prefixes_sorted:
                    if msisdn.startswith(prefix):
                        msisdn_prefix = prefixes[prefix]
                        break

                # Поиск префиксной зоны для номера DIALED
                for prefix in prefixes_sorted:
                    if dialed.startswith(prefix):
                        dialed_prefix = prefixes[prefix]
                        break

                # Обновление полей записи с префиксными зонами
                row[9] = msisdn_prefix or "Unknown"
                row[10] = dialed_prefix or "Unknown"

                # Запись обновленной строки в выходной файл CDR
                writer.writerow(row)

                # Обновление агрегированных данных о длительности соединений
                duration = int(row[8])
                if msisdn_prefix and dialed_prefix:
                    pair_found = False
                    for entry in aggregated_data:
                        if entry[0] == msisdn_prefix and entry[1] == dialed_prefix:
                            entry[2] += duration
                            pair_found = True
                            break
                    if not pair_found:
                        aggregated_data.append([msisdn_prefix, dialed_prefix, duration])

# Шаг 5: Запись агрегированных данных о длительности соединений в файл VOLUMES.TXT
with open(volumes_file, "w") as volumes_output:
    writer = csv.writer(volumes_output, delimiter=',', lineterminator='\n')
    writer.writerows(aggregated_data)
