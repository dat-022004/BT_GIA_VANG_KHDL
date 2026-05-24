# Hệ thống phân tích giá vàng bằng PySpark  
Link Youtube: https://youtu.be/uiCf-Wzj7do?si=RHPb-GC4a4ObTC27  
linK Slides : https://www.canva.com/design/DAHJD6UTypE/I5DQjYtD-KU12NH2fqjOXg/edit  
Repository này chứa một bài thực hành so sánh hai hướng xử lý dữ liệu:

- `PySpark` cho xử lý phân tán với Docker Spark Cluster.
- `Jupyter Lab` để chạy và trình bày kết quả trong notebook.

## 1. Cấu trúc hệ thống

- `download_data.py`: tải dữ liệu giá vàng và tạo file benchmark lớn.
- `docker-compose.yml`: dựng cụm Spark gồm `spark-master`, `spark-worker-1`, `spark-worker-2` và `jupyter`.
- `data/`: chứa dữ liệu đầu vào và dữ liệu benchmark.
- `thuyettrinh/`: chứa notebook phân tích.

### Thành phần Docker

- `spark-master`: cổng `7077` cho Spark và `8080` cho Spark Web UI.
- `spark-worker-1`: Spark worker thứ nhất, Web UI tại `8081`.
- `spark-worker-2`: Spark worker thứ hai, Web UI tại `8082`.
- `jupyter`: Jupyter Lab tại `http://localhost:8888/lab`.

Token Jupyter trong file compose hiện tại là `vang2024`.

## 2. Yêu cầu cài đặt

- Docker Desktop đã bật.
- Python 3.11 trở lên nếu muốn chạy script ngoài container.
- Các thư viện trong `requirements.txt`.

Nếu chạy trên Windows, nên mở PowerShell hoặc terminal VS Code ở thư mục gốc của dự án:

```powershell
cd "E:\hoctap\khoa_hoc_MT\thuc_hanh_1 - Copy"
```

Nếu chạy từ WSL, đường dẫn tương đương là:

```bash
cd "/mnt/e/hoctap/khoa_hoc_MT/thuc_hanh_1 - Copy"
```

## 3. Cài đặt môi trường Python

Tạo và kích hoạt virtual environment:

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
& ".venv\Scripts\Activate.ps1"
```

Cài đặt dependencies:

```powershell
pip install -r requirements.txt
```

## 4. Tạo dữ liệu

### 4.1 Tải dữ liệu gốc

Script `download_data.py` sẽ tải dữ liệu lịch sử giá vàng và lưu vào `data/gold_prices_all.csv`.

```powershell
python download_data.py
```

### 4.2 Tạo dữ liệu benchmark lớn

Để phục vụ so sánh Pandas vs Spark, script có thể tạo dataset synthetic kích thước lớn:

```powershell
python download_data.py --benchmark-rows 40000000
```

Kết quả đầu ra:

- `data/gold_prices_all.csv`
- `data/gold_prices_benchmark.parquet`

## 5. Khởi động hệ thống bằng Docker

Chạy toàn bộ stack:

```powershell
docker-compose up -d
```

Kiểm tra các container đang chạy:

```powershell
docker-compose ps
```

Theo dõi log của Jupyter:

```powershell
docker logs jupyter
```

### Truy cập giao diện

- Jupyter Lab: `http://localhost:8888/lab`
- Spark Master UI: `http://localhost:8080`
- Spark Worker 1 UI: `http://localhost:8081`
- Spark Worker 2 UI: `http://localhost:8082`

## 6. Cách chạy notebook

Notebook chính nằm trong `thuyettrinh/`.

- `thuyettrinh/pandas_analysis.ipynb`: phân tích bằng Pandas.
- `thuyettrinh/spark_analysis.ipynb`: phân tích bằng PySpark.
- `thuyettrinh/chuong3.ipynb`: nội dung thuyết trình / lý thuyết.

Trong Jupyter Lab:

1. Mở `http://localhost:8888/lab`.
2. Nhập token `vang2026`.
3. Mở notebook trong thư mục `/home/jovyan/work`.
4. Chạy tuần tự các cell từ trên xuống.

### Lưu ý đường dẫn dữ liệu

- Notebook Pandas và Spark nên đọc file bằng đường dẫn tương đối lên thư mục gốc, ví dụ `../data/gold_prices_benchmark.parquet`.
- Nếu mở notebook từ thư mục `thuyettrinh/`, đường dẫn `data/...` sẽ dễ bị sai.

## 7. Kết quả đạt được

### 7.1 Kết quả dữ liệu

- Đã tải dữ liệu giá vàng lịch sử vào `data/gold_prices_all.csv`.

### 7.2 Kết quả Pandas

Notebook Pandas thực hiện:

- nạp dữ liệu vào RAM bằng `pd.read_parquet()`
- làm sạch dữ liệu
- tính `daily_return_pct`
- group by theo tháng
- xuất bảng thống kê và biểu đồ

Điểm chính:

- phù hợp khi dữ liệu vừa và nhỏ
- thao tác đơn giản, dễ debug
- phụ thuộc mạnh vào RAM máy local

### 7.3 Kết quả PySpark

Notebook Spark thực hiện:

- khởi tạo `SparkSession`
- đọc dữ liệu bằng Spark DataFrame
- dùng `Window`, `groupBy`, `agg`, `collect`
- hiển thị thống kê theo tháng, theo năm và các biểu đồ so sánh

Điểm chính:

- phù hợp khi dữ liệu lớn
- có thể chạy trên cụm Docker nhiều worker
- hỗ trợ lazy evaluation và phân tán tính toán

## 8. Các tình huống thường gặp

### Không mở được `http://localhost:8888/lab`

- Kiểm tra Docker Desktop đã chạy chưa.
- Kiểm tra container `jupyter` đã up chưa.
- Xem log bằng `docker logs jupyter`.

### Notebook báo không tìm thấy file dữ liệu

- Đảm bảo đã chạy `download_data.py`.
- Kiểm tra `data/gold_prices_benchmark.parquet` có tồn tại.
- Mở notebook từ `thuyettrinh/` thì ưu tiên dùng `../data/...`.

### SparkSession lỗi khi khởi tạo

- Nếu chạy local, dùng `.master("local[*]")`.
- Nếu dùng cluster Docker, đảm bảo container `spark-master` và workers đang chạy.
- Xem log của Spark container để tìm lỗi kết nối hoặc port.

## 9. Tóm tắt nhanh lệnh sử dụng

```powershell
cd "E:\hoctap\khoa_hoc_MT\thuc_hanh_1 - Copy"
docker-compose up -d
docker logs jupyter
```
