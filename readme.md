
# Technical Test Data Engineer UMS - Movies DWH

Sebelum menjalankan script yang telah dibuat mohon perhatikan dulu apa saja yang saya gunakan dalam pengerjaan tes ini. Setidaknya pastikan Anda menggunakan tech stack versi yang sama terutama untuk versi Python & MySQL.




## Tech Stack

**OS:** macOS Ventura 13.2

**Python:** version 3.9.6

**DB** : MySQL 8.1


## Instalasi kebutuhan Python

Sebelum menginstalasi, kami sarankan buat dan jalankan dulu virtual environment untuk project ini. Untuk menginstall virtual environment itu anda dapat lihat di link di bawah ini :

- [Virtual Environment Python](https://docs.python.org/3.9/library/venv.html)

```bash
  cd movies_dwh_ums
  pip install -r requirements.txt
```
    
## Deployment

Asumsikan Anda sudah di dalam virtual environment yang telah dibuat. Anda tinggal jalankan file utama untuk pipeline. 

```bash
  python main_pipeline.py
```

Setelah itu coba cek database Anda, harusnya sudah bertambah data baru / memperbaharui data yang telah ada.