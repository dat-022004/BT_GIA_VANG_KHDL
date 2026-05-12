import os
import urllib.request

def setup_winutils():
    # Kiểm tra xem đang chạy trên Windows không
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        bin_dir = os.path.join(hadoop_home, 'bin')
        
        # Tạo thư mục nếu chưa có
        os.makedirs(bin_dir, exist_ok=True)
        
        winutils_path = os.path.join(bin_dir, 'winutils.exe')
        hadoop_dll_path = os.path.join(bin_dir, 'hadoop.dll')
        
        # Tải winutils.exe và hadoop.dll (Hadoop 3.2.2)
        base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/"
        
        if not os.path.exists(winutils_path):
            print("Đang tải winutils.exe (Yêu cầu cho PySpark trên Windows)...")
            urllib.request.urlretrieve(base_url + "winutils.exe", winutils_path)
            
        if not os.path.exists(hadoop_dll_path):
            print("Đang tải hadoop.dll (Yêu cầu cho PySpark trên Windows)...")
            urllib.request.urlretrieve(base_url + "hadoop.dll", hadoop_dll_path)
            
        # Thiết lập biến môi trường
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')
        
        # Bỏ qua SPARK_HOME toàn cục (nếu có) để ép script dùng bản 3.5.1 đã cài qua pip
        if 'SPARK_HOME' in os.environ:
            del os.environ['SPARK_HOME']
        
setup_winutils()
