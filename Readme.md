Project ETL: 
Extract data dari berbagai sumber ke dalam sqlite
Menggunakan Run Config Param untuk menentukan extract data apa dan dari jenis source apa
Masing masing jenis file di-extract menggunakan task yang berbeda (gunakan BranchOperator untuk memilih ekstraksi menggunakan task yang mana)
Simpan data hasil ekstrak ke dalam file berjenis sama di staging area (folder data/). contoh: parquet - semua data hasil ekstraksi akan disimpan sebagai parquet tidak peduli sourcenya dari mana
load data dari staging area ke dalam sqlite menggunakan task yang berbeda
file dikumpulkan bersama dengan screenshot parameter + current graph + folder data + hasil di sqlite