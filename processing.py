import pandas as pd
import sqlite3
import re
from keybert import KeyBERT
from spacy.lang.ru.stop_words import STOP_WORDS as RUSSIAN_STOP_WORDS
import spacy
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

# Загружаем модель spaCy для русского языка
nlp = spacy.load('ru_core_news_sm')


def load_data(db_name="vacancies.db"):
    """
    Загружает данные из SQLite в DataFrame.
    """
    conn = sqlite3.connect(db_name)
    query = "SELECT * FROM vacancies"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def clean_text(text):
    """
    Очищает текст от HTML-тегов и лишних пробелов.
    """
    if pd.isnull(text):
        return text
    text = re.sub(r'<[^>]+>', '', text)  # Удаляем HTML-теги
    text = re.sub(r'\s+', ' ', text).strip()  # Удаляем лишние пробелы
    return text


def clean_data(data):
    """
    Очищает текстовые поля данных.
    """
    data['snippet_requirement'] = data['snippet_requirement'].apply(clean_text)
    data['snippet_responsibility'] = data['snippet_responsibility'].apply(clean_text)
    return data


def lemmatize_text(text):
    """
    Лемматизирует текст с использованием spaCy.
    """
    doc = nlp(text)
    return ' '.join([token.lemma_ for token in doc])


def lemmatize_data(data):
    """
    Лемматизирует текстовые поля данных.
    """
    data['snippet_requirement'] = data['snippet_requirement'].apply(lemmatize_text)
    data['snippet_responsibility'] = data['snippet_responsibility'].apply(lemmatize_text)
    return data


def extract_keybert_keywords(text, model, ngram_range=(1, 3), top_n=20):
    """
    Извлекает ключевые слова из текста с помощью KeyBERT.
    """
    if pd.isnull(text) or len(text.strip()) == 0:
        return ''
    keywords = model.extract_keywords(
        text,
        keyphrase_ngram_range=ngram_range,
        stop_words=list(RUSSIAN_STOP_WORDS),
        top_n=top_n
    )
    return ', '.join([kw[0] for kw in keywords])


def extract_keywords(data):
    """
    Извлекает ключевые слова из текстовых полей данных.
    """
    model = KeyBERT(model='DeepPavlov/rubert-base-cased')

    tqdm.pandas(desc="Extracting keywords for requirements")
    data['keywords_requirement_keybert'] = data['snippet_requirement'].progress_apply(
        lambda x: extract_keybert_keywords(x, model)
    )

    tqdm.pandas(desc="Extracting keywords for responsibilities")
    data['keywords_responsibility_keybert'] = data['snippet_responsibility'].progress_apply(
        lambda x: extract_keybert_keywords(x, model)
    )
    return data


def process_data(db_name="vacancies.db"):
    """
    Полный цикл обработки данных: загрузка, очистка, лемматизация, извлечение ключевых слов.
    """
    data = load_data(db_name)
    print(f"Данные загружены: {data.shape}")
    data = clean_data(data)
    data = lemmatize_data(data)
    data = extract_keywords(data)
    return data
