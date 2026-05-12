# Báo cáo So sánh: Pandas vs. PySpark trên Cụm Phân tán (Mô phỏng `local[4]`)

## 1. Giới thiệu
Dự án này phân tích tập dữ liệu NYC Yellow Taxi Trip để tính toán "Doanh thu trung bình và thời gian di chuyển theo từng khung giờ trong ngày". 
Mục tiêu là so sánh hiệu năng và kiến trúc giữa **Pandas** (xử lý trên bộ nhớ đơn máy) và **PySpark** (chạy ở chế độ `local[4]` mô phỏng xử lý phân tán với 4 luồng xử lý song song).

## 2. Kết quả Thực thi

### 2.1. Pandas Analysis (Eager Execution)
- **Quy mô dữ liệu:** 1 tháng (Tháng 1/2024, ~50MB parquet, khoảng 3 triệu dòng).
- **Bộ nhớ sử dụng:** Tải toàn bộ vào RAM (In-memory).
- **Cách thức hoạt động:** Các câu lệnh như lọc (`filter`), tạo cột (`withColumn`), gom nhóm (`groupby`) được thực thi ngay lập tức tại thời điểm gọi.

*(Bạn có thể chạy `python pandas_analysis.py` và điền số liệu thời gian/RAM vào đây)*

### 2.2. PySpark Analysis (Lazy Evaluation)
- **Quy mô dữ liệu:** Nhiều tháng (tổng cộng ~300MB+ parquet).
- **Bộ nhớ sử dụng:** Không tải ngay vào RAM. Spark chỉ đọc schema và tạo một Execution Plan (DAG). Dữ liệu được xử lý stream từ đĩa theo từng block (partition).
- **Cách thức hoạt động:** Các câu lệnh `.filter()`, `.withColumn()` là các transformations trả về ngay lập tức. Tính toán chỉ thực sự xảy ra khi gọi action `.show()`. Công việc được chia cho 4 luồng (threads) thực thi song song trên máy tính nhờ `.master("local[4]")`.

*(Bạn có thể chạy `python spark_analysis.py` và điền số liệu thời gian vào đây)*

## 3. Đánh giá Kiến trúc

| Tiêu chí | Pandas | PySpark (Cluster / local[4]) |
| :--- | :--- | :--- |
| **Kiến trúc** | Đơn luồng (Single-threaded) | Phân tán (Multi-threaded/Multi-node) |
| **Quản lý bộ nhớ** | In-memory (Yêu cầu RAM > Kích thước dữ liệu x 5-10 lần) | Tối ưu Disk/RAM, xử lý từng Partition |
| **Thực thi** | Eager Execution (Tính toán ngay) | Lazy Evaluation (Trì hoãn tính toán, tối ưu DAG) |
| **Phù hợp cho** | Dữ liệu nhỏ, vừa (< 2-3GB), phân tích thăm dò | Big Data (hàng trăm GB, TB), hệ thống Data Pipeline lớn |

## 4. Tổng kết
Việc sử dụng cấu hình `.master("local[4]")` giúp chúng ta trải nghiệm sức mạnh xử lý song song của PySpark trên chính một máy tính cá nhân, mang lại cách tiếp cận tương tự như cấu hình một cụm gồm nhiều máy thật (standalone cluster) nhưng đơn giản hơn, không cần ảo hóa (VMware/VirtualBox). Bằng cơ chế chia nhỏ dữ liệu thành các Partitions và giao cho 4 worker threads, PySpark có khả năng xử lý lượng dữ liệu vượt quá dung lượng RAM một cách ổn định và nhanh chóng.
