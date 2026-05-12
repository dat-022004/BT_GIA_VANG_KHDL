import os
import requests
from tqdm import tqdm

# URL base của NYC TLC Yellow Taxi (2024)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

# Thư mục lưu dữ liệu
DATA_DIR = "data"

def download_file(url, filepath):
    if os.path.exists(filepath):
        print(f"File {filepath} đã tồn tại, bỏ qua...")
        return
    
    print(f"Đang tải {url}...")
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024 # 1 Kibibyte
        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True)
        
        with open(filepath, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        
        if total_size != 0 and progress_bar.n != total_size:
            print("ERROR: Có lỗi xảy ra trong quá trình tải.")
    else:
        print(f"ERROR: Không thể tải file từ {url} (Status: {response.status_code})")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    year = 2024
    
    # Tải 6 tháng đầu năm 2024 (Pandas sẽ chỉ dùng tháng 1, PySpark dùng cả 6 tháng)
    print("Bắt đầu tải dữ liệu NYC Taxi (tháng 1 đến tháng 6 năm 2024)...")
    for month in range(1, 7):
        url = BASE_URL.format(year=year, month=month)
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        filepath = os.path.join(DATA_DIR, filename)
        
        download_file(url, filepath)
        
    print("\nHoàn tất tải dữ liệu!")

if __name__ == "__main__":
    main()
