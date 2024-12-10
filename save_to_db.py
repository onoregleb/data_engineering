import sqlite3


def create_database(db_name="vacancies.db"):
    """
    Создает базу данных и таблицу для вакансий, если она не существует.
    """
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
    connection.close()


def save_to_sqlite(vacancies, db_name="vacancies.db"):
    """
    Сохраняет список вакансий в базу данных SQLite.
    """
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
            print(f"Вакансия с ID {vacancy.get('id')} уже существует в базе данных.")
        except sqlite3.Error as e:
            print(f"Ошибка при вставке данных вакансии {vacancy.get('id')}: {e}")

    connection.commit()
    connection.close()
    print(f"Успешно сохранено {len(vacancies)} вакансий в базу данных.")


if __name__ == "__main__":
    from data_api import fetch_vacancies

    # Параметры запроса
    params = {
        "text": "Data Scientist OR Machine Learning OR Аналитик данных OR Искусственный интеллект OR Data Engineer OR ML",
        "area": 113,  # Россия
        "specialization": 1,  # IT
    }

    # Создаем базу данных
    create_database()

    # Получение данных через API
    vacancies = fetch_vacancies(params)

    # Сохранение данных в SQLite
    save_to_sqlite(vacancies)
