import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from loguru import logger
import warnings
import re

warnings.filterwarnings("ignore")

logger.add("logging/etl_process.log", rotation="1 week", retention="10 days")  # Логи в файл


# Функция для загрузки данных из базы данных
def load_data(db_path):
    logger.info(f"Загрузка данных из базы данных: {db_path}")
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM vacancies"
    df = pd.read_sql_query(query, conn)
    conn.close()
    logger.info(f"Данные загружены. Размер данных: {df.shape}")
    return df


# Функция для получения курса обмена валюты по указанной дате и коду валюты
def get_exchange_rate(date, currency_code):
    logger.debug(f"Получение курса обмена для {currency_code} на {date}")
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date}"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "xml")
        for valute in soup.find_all("Valute"):
            if valute.CharCode.text == currency_code:
                exchange_rate = float(valute.Value.text.replace(",", ".")) / int(valute.Nominal.text)
                logger.debug(f"Курс для {currency_code} на {date}: {exchange_rate}")
                return exchange_rate
    logger.warning(f"Курс для {currency_code} на {date} не найден.")
    return 1  # Возвращаем 1, если курс не найден (например, для RUR или ошибок запроса)


# Функция для перевода зарплаты в RUR
def convert_salary_to_rur(row):
    if row['salary_currency'] != "RUR":
        exchange_rate = get_exchange_rate(row['published_at'].strftime("%d/%m/%Y"), row['salary_currency'])
    else:
        exchange_rate = 1

    # Переводим зарплату
    salary_from_rur = row['salary_from'] * exchange_rate if pd.notnull(row['salary_from']) else None
    salary_to_rur = row['salary_to'] * exchange_rate if pd.notnull(row['salary_to']) else None

    return pd.Series([salary_from_rur, salary_to_rur, "RUR"])


# Функция для очистки текста
def clean_text(text):
    if pd.isnull(text):
        return text
    text = re.sub(r'<[^>]+>', '', text)  # Удаляем HTML-теги
    text = re.sub(r'\s+', ' ', text).strip()  # Удаляем лишние пробелы
    return text


# Основная функция для обработки данных
def process_data(db_path):
    logger.info(f"Начинаем обработку данных из базы данных: {db_path}")

    # Загружаем данные
    data = load_data(db_path)

    # Преобразуем столбец даты
    logger.info("Преобразуем столбец 'published_at' в тип datetime.")
    data["published_at"] = pd.to_datetime(data["published_at"], errors="coerce")
    data["published_at"] = data["published_at"].dt.date

    # Очистка данных
    logger.info("Удаляем строки с пропущенными значениями в столбцах 'snippet_requirement' и 'snippet_responsibility'.")
    data = data.dropna(subset=['snippet_requirement', 'snippet_responsibility'])
    data['salary_to'] = data['salary_to'].fillna(data['salary_from'])
    data = data.dropna(subset=["salary_from"])

    # Очистка текстовых столбцов
    logger.info("Очищаем текстовые столбцы.")
    data['snippet_requirement'] = data['snippet_requirement'].apply(clean_text)
    data['snippet_responsibility'] = data['snippet_responsibility'].apply(clean_text)

    # Применяем функцию для перевода зарплаты в RUR
    logger.info("Переводим зарплату в валюту RUR.")
    data[['salary_from', 'salary_to', 'salary_currency']] = data.apply(convert_salary_to_rur, axis=1)

    # Просмотр результатов
    logger.info(f"Обработанные данные:\n{data.head()}")

    # Сохраняем обновленные данные обратно в базу данных
    logger.info("Обновляем данные в базе данных.")
    conn = sqlite3.connect(db_path)
    data.to_sql('vacancies', conn, if_exists='replace', index=False)  # Обновляем таблицу
    conn.close()

    logger.info("Данные обработаны и база данных обновлена.")
    return data
