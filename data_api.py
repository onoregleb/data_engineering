import logging
import requests


def fetch_vacancies(params, max_pages=20, per_page=100):
    """
    Собирает вакансии с HH.ru по заданным параметрам.
    """
    url = "https://api.hh.ru/vacancies"
    headers = {"User-Agent": "JobAnalysis/1.0 (example@gmail.com)"}
    all_vacancies = []

    for page in range(max_pages):
        params.update({"page": page, "per_page": per_page})
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            page_data = response.json()
            if "items" in page_data and page_data["items"]:
                all_vacancies.extend(page_data["items"])
                print(f"Добавлено {len(page_data['items'])} вакансий с страницы {page}.")
            else:
                print(f"Страница {page} не содержит вакансий.")
                break
        else:
            print(f"Ошибка при получении данных с API: {response.status_code}")
            response.raise_for_status()

    print(f"Всего вакансий собрано: {len(all_vacancies)}.")
    return all_vacancies
