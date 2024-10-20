import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib.request

def extract_data(parquet_file='/opt/airflow/dags/data/staging/kompas.parquet'):
    #Mengambil halaman berita populer
    url='https://www.kompas.com/'
    response = requests.get(url)

    #Memeriksa apakah permintaan berhasil
    if response.status_code == 200:
        soup = BeautifulSoup(response.content,'html.parser')

        #Mengambil judul berita
        news_kompas = []
        for item in soup.find_all('div', class_='mostItem'):
            link_tag = item.find('a')
            title_tag = item.find('h2', class_='mostTitle')
            group_tag = item.find('div', class_='mostChannel')

            if link_tag and title_tag and group_tag:         
                link = link_tag['href']
                title = title_tag.text.strip()
                group = group_tag.text.strip()
                
                #Mengambil tanggal berita terbit
                date_news = datetime.now().strftime('%Y-%m-%d')

                #Menyimpan data dalam list
                news_kompas.append({
                    'title': title,
                    'link': link,
                    'date_news': date_news,
                    'group_news': group
            })

            print(f'Saved: {title} on {date_news}')

  
        # Membuat DataFrame dan menyimpan sebagai Parquet
        df = pd.DataFrame(news_kompas)
        df.to_parquet(parquet_file, index=False) 

        print(f'Data saved to Parquet file: {parquet_file}')
    else:
        print('Error:', response.status_code)

