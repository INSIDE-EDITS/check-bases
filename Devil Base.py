import os
import subprocess
import sys
import requests
from colorama import init, Fore, Style

# Инициализация colorama
init(autoreset=True)

# URL репозитория на GitHub
REPO_URL = "https://github.com/INSIDE-EDITS/check-bases.git"
SCRIPT_NAME = "Devil Base.py"
CURRENT_VERSION = "v2"

def get_latest_version():
    try:
        # Получаем последнюю версию из репозитория
        response = requests.get(f"https://raw.githubusercontent.com/yourusername/yourrepository/main/{SCRIPT_NAME}")
        response.raise_for_status()
        content = response.text
        latest_version = content.split('\n')[0].split('=')[1].strip().strip('"')
        return latest_version
    except Exception as e:
        print(f"{Fore.RED}[-] Ошибка при получении последней версии: {str(e)}{Style.RESET_ALL}")
        return None

def check_for_updates():
    latest_version = get_latest_version()
    if latest_version is None:
        return

    if latest_version == CURRENT_VERSION:
        print(f"{Fore.LIGHTGREEN_EX}[+] Скрипт уже обновлен до последней версии ({CURRENT_VERSION}).{Style.RESET_ALL}")
    else:
        print(f"{Fore.LIGHTGREEN_EX}[+] Доступна новая версия скрипта ({latest_version}).{Style.RESET_ALL}")
        try:
            # Обновляем скрипт
            subprocess.run(["git", "pull"], check=True)
            print(f"{Fore.LIGHTGREEN_EX}[+] Скрипт обновлен до версии {latest_version}.{Style.RESET_ALL}")
            # Перезапускаем скрипт
            os.execv(__file__, sys.argv)
        except Exception as e:
            print(f"{Fore.RED}[-] Ошибка при обновлении скрипта: {str(e)}{Style.RESET_ALL}")

def main():
    # Проверяем наличие обновлений
    check_for_updates()

    # Ваш основной код скрипта
    import dask.dataframe as dd
    import glob
    import chardet
    import os
    import aiofiles
    import asyncio
    import re
    from colorama import init, Fore, Style

    # Инициализация colorama
    init(autoreset=True)

    # Логотип Devil Base
    logo = """

    ░▒▓███████▓▒░░▒▓████████▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░▒▓█▓▒░             ░▒▓███████▓▒░ ░▒▓██████▓▒░ ░▒▓███████▓▒░▒▓████████▓▒░
    ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░▒▓█▓▒░             ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░
    ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░       ░▒▓█▓▒▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░             ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░
    ░▒▓█▓▒░░▒▓█▓▒░▒▓██████▓▒░  ░▒▓█▓▒▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░             ░▒▓███████▓▒░░▒▓████████▓▒░░▒▓██████▓▒░░▒▓██████▓▒░
    ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░        ░▒▓█▓▓█▓▒░ ░▒▓█▓▒░▒▓█▓▒░             ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░      ░▒▓█▓▒░▒▓█▓▒░
    ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░        ░▒▓█▓▓█▓▒░ ░▒▓█▓▒░▒▓█▓▒░             ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░      ░▒▓█▓▒░▒▓█▓▒░
    ░▒▓███████▓▒░░▒▓████████▓▒░  ░▒▓██▓▒░  ░▒▓█▓▒░▒▓████████▓▒░      ░▒▓███████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓███████▓▒░░▒▓████████▓▒░
                                                                                                                       
                                                                                                                       
            OWHER @modeDOXBIN
             PROJECT - https://t.me/ostiner_bot

    """

    def print_centered(text):
        terminal_width = os.get_terminal_size().columns
        lines = text.split('\n')
        for line in lines:
            print(line.center(terminal_width))

    async def detect_encoding(file_path):
        async with aiofiles.open(file_path, 'rb') as f:
            content = await f.read()
        result = chardet.detect(content)
        return result['encoding']

    def remove_commas_in_names(text):
        # Регулярное выражение для поиска шаблона "Фамилия, Имя, Отчество"
        pattern = re.compile(r'(\w+),\s*(\w+),\s*(\w+)')
        return pattern.sub(r'\1 \2 \3', text)

    async def process_data(file_path, processed_bases, total_duplicates):
        try:
            print(f"\n{Fore.LIGHTGREEN_EX}[+] Обрабатывается база {os.path.basename(file_path)}{Style.RESET_ALL}")

            # Определяем кодировку файла
            charenc = await detect_encoding(file_path)

            # Считываем данные из файла с помощью dask
            chunksize = 10 ** 6
            ddf = dd.read_csv(file_path, sep='\t', encoding=charenc, blocksize=chunksize)

            # Удаляем запятые между фамилией, именем и отчеством
            ddf = ddf.map_partitions(lambda df: df.applymap(lambda x: remove_commas_in_names(x) if isinstance(x, str) else x))

            # Удаляем дубликаты
            original_count = len(ddf)
            ddf = ddf.drop_duplicates()
            duplicates_count = original_count - len(ddf)

            # Создаем папку "Cleaned Bases", если она не существует
            cleaned_dir = "Cleaned Bases"
            os.makedirs(cleaned_dir, exist_ok=True)

            # Сохраняем очищенные данные в новый файл в папке "Cleaned Bases"
            cleaned_file_path = os.path.join(cleaned_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}-Cleaned{os.path.splitext(file_path)[1]}")
            ddf.to_csv(cleaned_file_path, sep='\t', index=False, single_file=True)

            print(f"\n{Fore.LIGHTGREEN_EX}[+] База {os.path.basename(file_path)} отработана{Style.RESET_ALL}")

            # Обновляем счетчики
            processed_bases.append(file_path)
            total_duplicates.append(duplicates_count)

        except Exception as e:
            print(f"\n{Fore.RED}Ошибка при обработке файла {file_path}: {str(e)}{Style.RESET_ALL}")

    async def main():
        # Находим все файлы баз данных в текущем каталоге
        file_paths = glob.glob("*.txt") + glob.glob("*.csv") + glob.glob("*.xls") + glob.glob("*.xlsx")

        # Вывод количества загруженных баз
        print(f"\n{Fore.LIGHTGREEN_EX}[!] Загружено баз {len(file_paths)}!{Style.RESET_ALL}")

        # Счетчики для обработанных баз и дубликатов
        processed_bases = []
        total_duplicates = []

        # Создаем список задач для параллельной обработки
        tasks = [process_data(file_path, processed_bases, total_duplicates) for file_path in file_paths]

        # Запускаем задачи параллельно
        await asyncio.gather(*tasks)

        # Создание текстового файла с информацией в папке "Cleaned Bases"
        cleaned_dir = "Cleaned Bases"
        os.makedirs(cleaned_dir, exist_ok=True)
        summary_file_path = os.path.join(cleaned_dir, "summary.txt")
        with open(summary_file_path, 'w') as f:
            f.write(f"Обработано баз: {len(processed_bases)}\n")
            f.write(f"Общее количество удаленных дубликатов: {sum(total_duplicates)}\n")

        print(f"\n{Fore.LIGHTGREEN_EX}[!] Создан файл с информацией: {summary_file_path}{Style.RESET_ALL}")

    if __name__ == "__main__":
        print_centered(f"{Fore.LIGHTGREEN_EX}{logo}{Style.RESET_ALL}")
        asyncio.run(main())

if __name__ == "__main__":
    main()