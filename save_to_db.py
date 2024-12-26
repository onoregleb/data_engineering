import sqlite3
from loguru import logger
from data_api import fetch_vacancies


# Настройка логирования
logger.add("logging/etl_process.log", rotation="1 week", retention="10 days")


def create_database(db_name="vacancies.db"):
    """
    Создает базу данных и таблицу для вакансий, если она не существует.
    """
    logger.info(f"Создание базы данных и таблицы: {db_name}")
    try:
        connection = sqlite3.connect(db_name)
        cursor = connection.cursor()

        # Создание таблицы
        create_table_query = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id TEXT PRIMARY KEY,
            name TEXT,
            area_name TEXT,
            salary_from REAL,
            salary_to REAL,
            salary_currency TEXT,
            published_at TEXT,
            employer_name TEXT,
            alternate_url TEXT,
            snippet_requirement TEXT,
            snippet_responsibility TEXT,
            professional_roles TEXT,
            schedule TEXT,
            employment TEXT,
            experience TEXT
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        logger.info("База данных и таблица успешно созданы.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создании базы данных: {e}")
    finally:
        connection.close()


def save_to_sqlite(vacancies, db_name="vacancies.db"):
    """
    Сохраняет список вакансий в базу данных SQLite.
    """
    logger.info(f"Сохранение {len(vacancies)} вакансий в базу данных: {db_name}")
    try:
        connection = sqlite3.connect(db_name)
        cursor = connection.cursor()

        # SQL-запрос для вставки данных
        insert_query = """
        INSERT OR IGNORE INTO vacancies (
            id, name, area_name, salary_from, salary_to, salary_currency,
            published_at, employer_name,
            alternate_url, snippet_requirement, snippet_responsibility,
            professional_roles, schedule, employment, experience
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Обработка данных и запись в базу
        for vacancy in vacancies:
            salary = vacancy.get("salary", {}) or {}
            employer = vacancy.get("employer", {}) or {}
            snippet = vacancy.get("snippet", {}) or {}
            professional_roles = vacancy.get("professional_roles", []) or []

            data = (
                vacancy.get("id"),
                vacancy.get("name"),
                vacancy.get("area", {}).get("name"),
                salary.get("from"),
                salary.get("to"),
                salary.get("currency"),
                vacancy.get("published_at"),
                employer.get("name"),
                vacancy.get("alternate_url"),
                snippet.get("requirement"),
                snippet.get("responsibility"),
                ", ".join([role["name"] for role in professional_roles]),
                vacancy.get("schedule", {}).get("name"),
                vacancy.get("employment", {}).get("name"),
                vacancy.get("experience", {}).get("name"),
            )
            try:
                cursor.execute(insert_query, data)
            except sqlite3.IntegrityError:
                logger.warning(f"Вакансия с ID {vacancy.get('id')} уже существует в базе данных.")
            except sqlite3.Error as e:
                logger.error(f"Ошибка при вставке данных вакансии {vacancy.get('id')}: {e}")

        connection.commit()
        logger.info(f"Успешно сохранено {len(vacancies)} вакансий в базу данных.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при работе с базой данных: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    # Параметры запроса
    params = {
        "text": "Data Scientist OR Machine Learning OR Аналитик данных OR Искусственный интеллект OR Data Engineer OR ML",
        "area": 113,  # Россия
        "specialization": 1,  # IT
    }

    try:
        # Создаем базу данных
        create_database()

        # Получение данных через API
        vacancies = fetch_vacancies(params)

        # Сохранение данных в SQLite
        save_to_sqlite(vacancies)
    except Exception as e:
        logger.error(f"Ошибка выполнения основного процесса: {e}")
