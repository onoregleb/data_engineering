import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px

# Подключение к базе данных
DB_PATH = r"C:\Users\Gleb Onore\Desktop\data_eng\vacancies.db"


@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM vacancies"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Загрузка данных
df = load_data()

# Предобработка данных
df['published_at'] = pd.to_datetime(df['published_at'])
df['salary_avg'] = df[['salary_from', 'salary_to']].mean(axis=1)

# Основной интерфейс
st.title("Анализ рынка вакансий HH.ru")
st.sidebar.header("Фильтры")

# Фильтры
selected_region = st.sidebar.multiselect(
    "Выберите регионы",
    options=df['area_name'].unique(),
    default=df['area_name'].unique()
)

selected_experience = st.sidebar.multiselect(
    "Выберите уровень опыта",
    options=df['experience'].unique(),
    default=df['experience'].unique()
)

# Применение фильтров
filtered_data = df[
    (df['area_name'].isin(selected_region)) &
    (df['experience'].isin(selected_experience))
    ]

# Основные метрики
st.metric("Всего вакансий", len(filtered_data))
st.metric("Средняя зарплата", f"{filtered_data['salary_avg'].mean():,.2f} ₽")
st.metric("Количество регионов", filtered_data['area_name'].nunique())

# 1. Зависимость зарплаты от опыта
if st.checkbox("Показать зависимость зарплаты от опыта"):
    experience_salary = filtered_data.groupby('experience')['salary_avg'].mean().reset_index()

    fig = px.bar(
        experience_salary,
        x="experience",
        y="salary_avg",
        title="Зависимость зарплаты от опыта",
        labels={"experience": "Опыт работы", "salary_avg": "Средняя зарплата"},
        color="salary_avg",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig)

# 2. Средняя зарплата по регионам
if st.checkbox("Показать среднюю зарплату по регионам"):
    region_salary = filtered_data.groupby('area_name')['salary_avg'].mean().reset_index()
    top_regions = region_salary.nlargest(10, 'salary_avg')

    fig = px.bar(
        top_regions,
        x="salary_avg",
        y="area_name",
        orientation="h",
        title="Средняя зарплата по регионам (Топ-10)",
        labels={"salary_avg": "Средняя зарплата", "area_name": "Регион"},
        color="salary_avg",
        color_continuous_scale="Reds"
    )

    # Инвертируем ось Y
    fig.update_layout(
        yaxis=dict(
            categoryorder="total ascending"  # Инвертируем порядок
        )
    )

    st.plotly_chart(fig)


# 3. Популярные профессиональные роли со средней зарплатой
if st.checkbox("Показать популярные профессиональные роли с средней зарплатой"):
    # Считаем количество вакансий по ролям и среднюю зарплату по ролям
    role_stats = filtered_data.groupby('professional_roles').agg(
        salary_avg=('salary_avg', 'mean'),
        count=('salary_avg', 'size')
    ).reset_index()

    # Отбираем топ-10 популярных ролей
    top_roles = role_stats.nlargest(10, 'count')

    # Строим график
    fig = px.bar(
        top_roles,
        x="count",
        y="professional_roles",
        orientation="h",
        title="Популярные профессиональные роли с средней зарплатой (Топ-10)",
        labels={"count": "Количество вакансий", "professional_roles": "Роль"},
        color="salary_avg",
        color_continuous_scale="Oranges",
        hover_data={"salary_avg": True, "count": True}  # Показываем среднюю зарплату при наведении
    )

    # Инвертируем ось Y для лучшего отображения
    fig.update_layout(
        yaxis=dict(
            categoryorder="total ascending"  # Инвертируем порядок
        )
    )
    st.plotly_chart(fig)

