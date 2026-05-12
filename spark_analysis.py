from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, avg, count
import time
import os

def main():
    print("=== PYSPARK ANALYSIS (6 MONTHS) ===")
    print("Cơ chế: Lazy Evaluation, Xử lý phân tán trên Disk/RAM")
    print("Cluster Mode: Cụm phân tán thực tế (Docker) - 1 Master, 2 Workers")
    
    # Khởi tạo Spark Session trỏ tới Master của Docker
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Analysis") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "4") \
        .getOrCreate()
        
    start_time = time.time()
    
    # Đường dẫn thư mục được mount bên trong Docker container
    data_dir = "/opt/spark-data"
    
    print("\n1. Tạo DataFrame và xây dựng Execution Plan (Lazy Evaluation)...")
    # Bước này trả về ngay lập tức vì Spark chưa thực sự đọc dữ liệu (ngoại trừ đọc schema)
    df = spark.read.parquet(data_dir)
    
    # 2. Xây dựng chuỗi các transformation
    df_filtered = df.filter((col("total_amount") > 0) & (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")))
    
    # Tính thời gian di chuyển (phút) = (dropoff - pickup) / 60
    duration_expr = (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
    
    df_transformed = df_filtered \
        .withColumn("duration_minutes", duration_expr) \
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        
    # Gom nhóm
    result = df_transformed.groupBy("pickup_hour").agg(
        avg("total_amount").alias("avg_revenue"),
        avg("duration_minutes").alias("avg_duration_minutes"),
        count("*").alias("trip_count")
    ).orderBy("pickup_hour")
    
    print("Spark đã xây dựng xong DAG (Directed Acyclic Graph) nhưng chưa tính toán gì!")
    
    print("\n2. Gọi ACTION để kích hoạt tính toán phân tán...")
    # Việc gọi .show() (Action) sẽ kích hoạt toàn bộ các Transformation bên trên
    result.show(5)
    
    # In ra execution plan để thấy rõ kiến trúc
    print("\n[Physical Plan]")
    result.explain()
    
    end_time = time.time()
    
    print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây")
    spark.stop()

if __name__ == "__main__":
    main()
