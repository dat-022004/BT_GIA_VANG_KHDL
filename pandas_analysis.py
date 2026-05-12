import pandas as pd
import time
import os
import psutil

def print_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    print(f"Memory Usage: {mem_info.rss / (1024 ** 2):.2f} MB")

def main():
    print("=== PANDAS ANALYSIS (1 MONTH) ===")
    print("Cơ chế: Eager Execution, Xử lý In-Memory trên 1 luồng")
    
    file_path = "data/yellow_tripdata_2024-01.parquet"
    if not os.path.exists(file_path):
        print(f"Lỗi: Không tìm thấy file {file_path}. Vui lòng chạy download_data.py trước.")
        return
        
    start_time = time.time()
    
    print("\n1. Đọc dữ liệu vào RAM...")
    df = pd.read_parquet(file_path)
    print_memory_usage()
    print(f"Số dòng dữ liệu: {len(df):,}")
    
    print("\n2. Thực hiện tính toán (Lọc, Tạo cột mới, Gom nhóm)...")
    # Lọc các chuyến đi có doanh thu > 0 và thời gian có ý nghĩa
    df_filtered = df[(df['total_amount'] > 0) & (df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime'])].copy()
    
    # Tính thời gian di chuyển (tính bằng phút)
    df_filtered['duration_minutes'] = (df_filtered['tpep_dropoff_datetime'] - df_filtered['tpep_pickup_datetime']).dt.total_seconds() / 60
    
    # Lấy giờ trong ngày từ thời gian đón
    df_filtered['pickup_hour'] = df_filtered['tpep_pickup_datetime'].dt.hour
    
    # Gom nhóm theo giờ, tính trung bình doanh thu và thời gian di chuyển
    result = df_filtered.groupby('pickup_hour').agg(
        avg_revenue=('total_amount', 'mean'),
        avg_duration_minutes=('duration_minutes', 'mean'),
        trip_count=('total_amount', 'count')
    ).reset_index()
    
    # Sắp xếp theo giờ
    result = result.sort_values('pickup_hour')
    
    print("\nKết quả (5 khung giờ đầu tiên):")
    print(result.head())
    
    end_time = time.time()
    
    print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây")
    print_memory_usage()
    print("=" * 40)

if __name__ == "__main__":
    main()
