import sqlite3
import pandas as pd
from processing import process_data  # или импортируйте вашу функцию обработки данных


# Функция для обновления данных в базе данных
def update_vacancies_in_db(processed_data, db_name="vacancies.db"):
    """
    Обновляет данные в базе данных SQLite.
    """
    # Подключаемся к базе данных
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    # Обновляем данные в таблице 'vacancies'
    for _, row in processed_data.iterrows():
        # Параметры для обновления
        update_query = """
        UPDATE vacancies
        SET snippet_requirement = ?,
            snippet_responsibility = ?,
            keywords_requirement_keybert = ?,
            keywords_responsibility_keybert = ?
        WHERE id = ?
        """

        # Подготовка данных для обновления
        data = (
            row['snippet_requirement'],  # Обновление snippet_requirement
            row['snippet_responsibility'],  # Обновление snippet_responsibility
            row['keywords_requirement_keybert'],  # Обновление ключевых слов для requirements
            row['keywords_responsibility_keybert'],  # Обновление ключевых слов для responsibilities
            row['id']  # Идентификатор вакансии
        )

        try:
            cursor.execute(update_query, data)
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении вакансии с ID {row['id']}: {e}")
            continue

    # Сохраняем изменения
    connection.commit()
    connection.close()
    print(f"База данных обновлена с {len(processed_data)} вакансиями.")


def main():
    # Обрабатываем данные (если необходимо, можете передать сюда аргументы или параметры)
    processed_data = process_data()  # Предположим, process_data() возвращает обработанный DataFrame

    # Обновляем данные в базе данных
    update_vacancies_in_db(processed_data)


if __name__ == "__main__":
    main()
