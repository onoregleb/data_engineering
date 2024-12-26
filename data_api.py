import time
import requests
from loguru import logger

# Настройка логирования
logger.add("logging/etl_process.log", rotation="1 week", retention="10 days")

def fetch_vacancies(params, max_pages=20, per_page=100):
    """
    Собирает вакансии с HH.ru по заданным параметрам.
    """
    url = "https://api.hh.ru/vacancies"
    headers = {"User-Agent": "JobAnalysis/1.0 (example@gmail.com)"}
    all_vacancies = []

    logger.info(f"Запуск сбора вакансий с параметрами: {params}")

    for page in range(max_pages):
        params.update({"page": page, "per_page": per_page})
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            page_data = response.json()

            if "items" in page_data and page_data["items"]:
                all_vacancies.extend(page_data["items"])

            # Логгирование каждых 5 страниц
            if (page + 1) % 5 == 0:
                logger.info(f"Обработано {page + 1} страниц.")

            # Если на странице нет вакансий, завершаем сбор
            if not page_data["items"]:
                logger.info(f"Страница {page} не содержит вакансий. Завершаем сбор.")
                break

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при получении данных с API на странице {page}: {e}")
            break

        # Пауза между запросами, чтобы избежать превышения лимитов API
        time.sleep(1)

    logger.info(f"Всего вакансий собрано: {len(all_vacancies)}.")
    return all_vacancies