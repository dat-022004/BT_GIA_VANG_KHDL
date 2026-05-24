"""
Tải giá vàng Việt Nam (5 năm gần đây) → data/gold_prices_all.csv
Nguồn: Yahoo Finance  GC=F (USD/oz) × USDVND=X → VNĐ/lượng
Cột: Ngay | Nam | Gia_Mua | Gia_Ban | Gia_TrungBinh | Nguon

Dùng: python download_data.py
"""

import os
from datetime import datetime
import pandas as pd
import requests

# ── Cấu hình ──────────────────────────────────────────────────
FILE_CSV      = os.path.join("data", "gold_prices_all.csv")
SO_NAM        = 5                  # Số năm muốn lấy dữ liệu
OZ_TREN_LUONG = 37.5 / 31.1035    # 1 lượng VN ≈ 1.2057 troy oz
# ──────────────────────────────────────────────────────────────


def _lay_yahoo(ma_chung_khoan: str, t1: int, t2: int) -> pd.Series:
    """Tải giá đóng cửa hàng ngày từ Yahoo Finance."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ma_chung_khoan}"
        f"?period1={t1}&period2={t2}&interval=1d"
    )
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    pts = r.json()["chart"]["result"][0]
    idx = pd.to_datetime(pts["timestamp"], unit="s").normalize()
    val = pts["indicators"]["quote"][0]["close"]
    return pd.to_numeric(pd.Series(val, index=idx), errors="coerce").dropna()


def main():
    hom_nay  = datetime.utcnow()
    ngay_dau = hom_nay.replace(year=hom_nay.year - SO_NAM)
    t1 = int(ngay_dau.timestamp())
    t2 = int(hom_nay.timestamp())

    print(f"Đang tải dữ liệu: {ngay_dau.date()} → {hom_nay.date()} ...")

    vang_usd = _lay_yahoo("GC=F",      t1, t2)   # Giá vàng USD/oz
    ti_gia   = _lay_yahoo("USDVND=X",  t1, t2)   # Tỷ giá VND/USD

    # Tính giá VNĐ/lượng  =  (USD/oz) × (VND/USD) × (oz/lượng)
    hop     = pd.concat([vang_usd.rename("vang"), ti_gia.rename("vnd")], axis=1).dropna()
    gia_vnd = hop["vang"] * OZ_TREN_LUONG * hop["vnd"]

    df = gia_vnd.reset_index()
    df.columns = ["Ngay", "Gia_TrungBinh"]
    df["Gia_Mua"] = df["Gia_TrungBinh"] * 1.005    # ước tính spread ±0.5%
    df["Gia_Ban"] = df["Gia_TrungBinh"] * 0.995
    df["Nguon"]   = "Yahoo Finance – GC=F × USDVND (VNĐ/lượng)"

    # Chuẩn hoá
    df["Ngay"] = pd.to_datetime(df["Ngay"]).dt.strftime("%Y-%m-%d")
    df["Nam"]  = pd.to_datetime(df["Ngay"]).dt.year
    df = df[["Ngay", "Nam", "Gia_Mua", "Gia_Ban", "Gia_TrungBinh", "Nguon"]]

    os.makedirs("data", exist_ok=True)
    df.to_csv(FILE_CSV, index=False, encoding="utf-8-sig")

    print(f"✓ Đã lưu : {FILE_CSV}")
    print(f"  Số ngày : {len(df):,}")
    print(f"  Từ ngày : {df['Ngay'].min()}  →  {df['Ngay'].max()}")
    print(f"  Giá gần nhất:")
    print(f"    Mua : {df['Gia_Mua'].iloc[-1]:>15,.0f} VNĐ/lượng")
    print(f"    Bán : {df['Gia_Ban'].iloc[-1]:>15,.0f} VNĐ/lượng")


if __name__ == "__main__":
    main()
