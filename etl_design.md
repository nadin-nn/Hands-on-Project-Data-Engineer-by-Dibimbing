# ETL Pipeline Design: E-Commerce Orders

## 1. Overview
Pipeline ini dibuat untuk memproses data transaksi penjualan e-commerce harian. Tugas utamanya adalah menggabungkan data transaksi dan katalog produk, membersihkan anomali data, serta membuat laporan ringkasan penjualan harian. Berhubung Apache Airflow cukup berat dijalankan di Windows, proyek ini menggunakan Prefect Cloud sebagai alternatif orchestrator yang lebih ringan. 

## 2. Extract
- Sumber: File penyimpanan lokal pada folder proyek (./orchestrate).
- Format: Dua file data mentah berformat CSV, yaitu 'raw_orders.csv' (data transaksi) dan 'raw_products.csv' (katalog produk). 
- Volume: Dataset transaksional harian berskala kecil hingga menengah yang dibaca menggunakan library Pandas menjadi objek DataFrame Python. 

## 3. Transform
- Langkah 1: Hapus Duplikasi Data
  Sistem otomatis mendeteksi dan menghapus baris transaksi yang kembar agar tidak terjadi perhitungan ganda pada total penjualan harian.
- Langkah 2: Filter Harga Negatif
  Menghapus baris transaksi yang memiliki nilai nominal di bawah nol (negatif) karena nilai harga negatif dikategorikan sebagai data error atau anomali sistem yang dapat membuat laporan menjadi tidak akurat. 
- Langkah 3: Mengisi Data Kosong
  Sistem mengisi data email pelanggan yang kosong pada kolom customer_email, serta mengisi kolom total_harga yang kosong menggunakan nilai tengah (median) dari keseluruhan data harga agar tidak ada data bolong yang hilang saat laporan akhir diekspor. 
- Langkah 4: Standarisasi Format Tanggal
  Mengubah tipe data string pada kolom tanggal_order menjadi format tanggal resmi Python (datetime) supaya data tanggal bisa diurutkan dan bisa digunakan untuk analisis tren waktu ke depan. 
- Langkah 5: Standarisasi Teks
  Membersihkan spasi hantu di ujung teks dan mengubah format penulisan kolom kota serta channel menjadi huruf kecil lalu diawali huruf kapital supaya menghindari duplikasi nama akibat salah ketik (misalnya membedakan "jakarta " dengan "Jakarta"), sehingga menjadi rapi.
- Langkah 6: Validasi Kualitas Akhir
  Memastikan tabel data yang sudah melalui 5 tahap pembersihan di atas tidak kosong total sebelum masuk ke tahap penyimpanan.

## 4. Load
- Tujuan: Folder lokal proyek sebagai simulasi Data Warehouse. 
- Format output:
  - orders_clean.csv: Dataset transaksi gabungan yang sudah bersih dan terstruktur.
  - summary_report.csv: Laporan yang menghitung total item terjual (total_items_sold) dan total pendapatan (total_revenue) untuk setiap produk sebagai bahan laporan.

## 5. Orchestration
- Tool: Prefect Cloud dan VS Code
- Schedule: Menggunakan ekspresi Cron 0 6 * * * yang artinya alur kerja ini otomatis berjalan sendiri setiap hari tepat pada jam 06:00 WIB pagi.
- DAG flow: Mulai (Start) ➔ Extract Orders & Products ➔ Transform and Clean Data ➔ Validate Quality ➔ Load to Warehouse ➔ Generate Report ➔ Send Notification (Slack) ➔ Selesai (End).

## 6. Error Handling
- Skenario 1: File Sumber Tidak Ditemukan 
  Jika salah satu file CSV mentah hilang dari folder, tahap Extract akan langsung memicu status Failed (Gagal) dan menghentikan seluruh alur kerja seketika untuk mencegah error beruntun di langkah selanjutnya.
- Skenario 2: Kegagalan Sistem Sementara (Network/Resource Glitch)
  Jika terjadi gangguan jaringan internet saat sinkronisasi cloud, Prefect dikonfigurasi untuk melakukan Retry (mencoba jalan ulang) secara otomatis sebanyak 3 kali (retries: 3) dengan jeda waktu tunggu 5 menit (retry_delay) di setiap percobaannya sebelum mengirim sinyal kritis.

## 7. Monitoring
- Bagaimana cara tahu pipeline sukses?
  Melalui dasbor Prefect Cloud (pada menu Flow Runs), eksekusi alur kerja akan ditandai dengan indikator warna hijau cerah dan berstatus Completed. Seluruh riwayat aktivitas teknis terekam lengkap pada tab Logs serta diekspor ke file pipeline_log.txt.
- Bagaimana cara tahu data berkualitas?
  Tahap Validate Quality bertugas menguji kualitas data sebelum disimpan. Lahirnya dua file output baru (orders_clean.csv dan summary_report.csv) di folder proyek dengan struktur tabel yang lengkap dan tidak kosong menjadi bukti fisik bahwa data yang masuk telah lolos uji kualitas secara otomatis.
