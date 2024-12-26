from prefect import flow, task
from data_api import fetch_vacancies
from save_to_db import create_database, save_to_sqlite
from data_processing import process_data


# Задачи для работы с базой данных и получения данных
@task
def fetch_data_task(params, max_pages=20, per_page=100):
    """
    Задача Prefect для получения данных с HH.ru.
    """
    return fetch_vacancies(params, max_pages, per_page)


@task
def save_to_db_task(vacancies, db_path):
    """
    Задача Prefect для сохранения данных в базу SQLite.
    """
    save_to_sqlite(vacancies, db_path)


@task
def initialize_db_task(db_path):
    """
    Задача Prefect для инициализации базы данных.
    """
    create_database(db_path)


@task
def process_data_task(db_path):
    """
    Задача Prefect для обработки данных.
    """
    process_data(db_path)


@flow
def main_flow():
    """
    Основной поток Prefect для выполнения всех задач.
    """
    # Параметры запроса
    params = {
        "text": "Data Scientist OR Machine Learning OR Аналитик данных OR Искусственный интеллект OR Data Engineer OR ML",
        "area": 113,  # Россия
        "specialization": 1,  # IT
    }

    # Путь к базе данных
    db_path = "vacancies.db"

    # Инициализация базы данных
    initialize_db_task(db_path)

    # Получение данных через API
    vacancies = fetch_data_task(params)

    # Сохранение данных в базу данных
    save_to_db_task(vacancies, db_path)

    # Обработка данных после их загрузки и сохранения в базе данных
    process_data_task(db_path)


if __name__ == "__main__":
    main_flow()
