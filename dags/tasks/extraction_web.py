import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib.request

def extract_data(parquet_file='data/news_data.parquet'):
    #Mengambil halaman berita populer
    url='https://www.detik.com/terpopuler'
    response = requests.get(url)

    #Memeriksa apakah permintaan berhasil
    if response.status_code == 200:
        soup = BeautifulSoup(response.content,'html.parser')

        #Mengambil judul berita
        news_data = []
        for item in soup.find_all('h3', class_='media__title'):
            link = item.find('a')['href']
            title = item.find('a').text.strip()

            #Mengambil tanggal berita terbit
            date_news = item.find_next('span', class_='media__date')
            if date_news:
                date_news = date_news.get_text().strip()
            else:
                date_news = datetime.now().strftime('%Y-%m-%d')

            #Menyimpan data dalam list
            news_data.append({
                'title': title,
                'link': link,
                'date_news': date_news
            })

            print(f'Saved: {title} on {date_news}')

        # Membuat DataFrame dan menyimpan sebagai Parquet
        df = pd.DataFrame(news_data)
        df.to_parquet(parquet_file, index=False)

        print(f'Data saved to SQLite and Parquet file: {parquet_file}')
    else:
        print('Error:', response.status_code)

