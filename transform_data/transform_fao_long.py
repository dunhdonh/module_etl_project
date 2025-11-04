# transform_data/transform_fao_long.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import FloatType
import os
import shutil
import re

# === 1. Config ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "input_data", "data_fao")
OUTPUT_DIR = os.path.join(BASE_DIR, "output_data", "fao_long_cleaned")

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === 2. Khởi tạo Spark ===
spark = SparkSession.builder.appName("FAO Wide to Long").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

csv_files = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError("❌ Không có file CSV nào trong data_input/data_fao")

df_all = None

# === 3. Xử lý từng file CSV ===
for file_path in csv_files:
    print(f"[INFO] 🔍 Đang xử lý file: {os.path.basename(file_path)}")
    df = spark.read.option("header", True).csv(file_path)

    # Tìm các cột dạng "YYYY value" và "YYYY flag"
    value_cols = [c for c in df.columns if re.match(r"^\d{4} value$", c)]
    flag_cols = [c for c in df.columns if re.match(r"^\d{4} flag$", c)]

    if not value_cols:
        print(f"[WARN] ⚠️ Không tìm thấy cột năm trong {file_path}. Các cột hiện có:")
        print(df.columns)
        continue

    # === 4. Chuyển từng năm thành dạng long ===
    df_long_parts = []
    for val_col in value_cols:
        year = int(val_col.split()[0])
        flag_col = f"{year} flag"

        df_y = df.select(
            col("Country Name En").alias("country"),
            col("Unit Name").alias("unit"),
            lit(year).alias("year"),
            col(val_col).cast(FloatType()).alias("value"),
            col(flag_col).alias("flag")
        )
        df_long_parts.append(df_y)

    # Hợp tất cả các năm trong 1 file
    df_long = df_long_parts[0]
    for d in df_long_parts[1:]:
        df_long = df_long.unionByName(d)

    # Loại bỏ null
    df_long_clean = df_long.dropna()

    # Thêm vào df_all tổng
    df_all = df_long_clean if df_all is None else df_all.unionByName(df_long_clean)

# === 5. Xuất kết quả theo từng năm ===
if df_all is None:
    raise ValueError("❌ Không có dữ liệu hợp lệ sau khi transform.")

years = [r["year"] for r in df_all.select("year").distinct().collect()]
print(f"[INFO] Tổng số năm: {len(years)} ({min(years)} → {max(years)})")

for year in years:
    year_str = str(year)
    temp_dir = os.path.join(OUTPUT_DIR, f"temp_{year_str}")
    final_csv = os.path.join(OUTPUT_DIR, f"{year_str}.csv")

    df_all.filter(col("year") == year) \
        .repartition(1) \
        .write.option("header", True).mode("overwrite").csv(temp_dir)

    # Đổi tên file CSV
    for f in os.listdir(temp_dir):
        if f.endswith(".csv"):
            os.replace(os.path.join(temp_dir, f), final_csv)
    shutil.rmtree(temp_dir)

spark.stop()
print("✅ Transform hoàn tất, dữ liệu đã lưu trong output_data/fao_long_cleaned/")
