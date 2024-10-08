import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

#Buat koneksi ke SQLite
conn = sqlite3.connect('news.db')
c= conn.cursor()

#Buat tabel untuk menampung data
c.execute('DROP TABLE IF EXISTS news')

c.execute('''
CREATE TABLE news(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  link TEXT NOT NULL,
  date_news DATETIME NOT NULL
)
''')

#Mengambil halaman berita populer
url='https://www.detik.com/terpopuler'
response = requests.get(url)

#Memeriksa apakah permintaan berhasil
if response.status_code == 200:
  soup = BeautifulSoup(response.content,'html.parser')

  #Mengambil judul berita
  for item in soup.find_all('h3', class_='media__title'):
      link = item.find('a')['href']
      title = item.find('a').text.strip()

      #Mengambil tanggal berita terbit
      date_news = item.find_next('span', class_='media__date')
      if date_news:
        date_news = date_news.get_text().strip()
      else:
        date_news = datetime.now().strftime('%Y-%m-%d')

      #Menyimpan data dalam database
      c.execute('INSERT INTO news (title, link, date_news) VALUES(?,?,?)',(title, link, date_news))
      print(f'Saved: {title} on {date_news}')

  #Comit perubahan ke database
  conn.commit()

else:
  print('Error:',response.status_code)

#Tutup koneksi ke database
conn.close()
