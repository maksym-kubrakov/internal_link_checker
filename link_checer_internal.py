import streamlit as st
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import os
from urllib.parse import urljoin, urlparse
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import tempfile
import shutil
import json
from openpyxl import Workbook
from openpyxl.styles import Alignment, PatternFill, Border, Side, Font

# Set NLTK data path
nltk_data_path = os.path.join(os.path.dirname(__file__), "nltk_data")
os.makedirs(nltk_data_path, exist_ok=True)
nltk.data.path.append(nltk_data_path)

# Download NLTK resources if not present
try:
    nltk.data.find('tokenizers/punkt_tab')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt_tab', download_dir=nltk_data_path, quiet=True)
    nltk.download('stopwords', download_dir=nltk_data_path, quiet=True)

class BacklinkCheckerApp:
    # [Весь код класу BacklinkCheckerApp з попередньої відповіді без змін]
    # Вставте код класу тут (починаючи з `def __init__(self):` до кінця класу)

# Streamlit інтерфейс
st.title("Backlink Checker")
st.write("Інструмент для перевірки внутрішніх посилань на сайті")

# Введення URL
urls_input = st.text_area("Введіть URL-адреси (по одному на рядок):", height=100)
upload_csv = st.file_uploader("Завантажте CSV-файл з URL (з колонкою 'URL')", type=["csv"])

# Формат експорту
export_format = st.radio("Формат експорту:", ["xlsx", "csv"], index=0)

# Прогрес
progress_bar = st.progress(0)
status_text = st.empty()

# Кнопка запуску
if st.button("Запустити перевірку"):
    app = BacklinkCheckerApp()
    
    # Обробка введених URL або CSV
    if upload_csv:
        df = pd.read_csv(upload_csv)
        if 'URL' in df.columns:
            app.urls = list(set(df['URL'].dropna().astype(str).tolist()))
        else:
            st.error("CSV-файл повинен містити колонку 'URL'.")
            st.stop()
    elif urls_input:
        app.urls = list(set(url.strip() for url in urls_input.splitlines() if url.strip()))
    else:
        st.warning("Введіть URL-адреси або завантажте CSV-файл.")
        st.stop()

    # Запуск перевірки
    def update_progress(message):
        status_text.text(message)

    with st.spinner("Обробка..."):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(app.check_all_urls(progress_callback=update_progress))
    
    # Відображення результатів
    if results:
        st.subheader("Результати перевірки")
        df = pd.DataFrame(results)
        st.dataframe(df[["URL", "HasLinks", "HasContentLinks", "ContentSource", "AnchorText", "Notes"]])
        
        # Текстове поле для ручного копіювання
        clipboard_text = df[["URL", "HasLinks", "HasContentLinks", "ContentSource", "AnchorText", "Notes"]].to_csv(index=False, sep="\t")
        st.text_area("Скопіюйте результати звідси:", clipboard_text, height=200)
        
        # Експорт результатів
        output_path = f"backlink_report.{export_format}"
        app.export_results(output_path, export_format)
        with open(output_path, "rb") as file:
            st.download_button(
                label="Завантажити звіт",
                data=file,
                file_name=output_path,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if export_format == "xlsx" else "text/csv"
            )
