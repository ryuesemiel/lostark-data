import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Parquet 파일 경로
file1_path = "aegirdata_20241231/Aegir_G1_Normal.parquet"

# Parquet 파일 읽기
df1 = pd.read_parquet(file1_path)

# 직업별 데이터 확인
job_column = "spec"  # 직업 데이터를 나타내는 열
if job_column in df1.columns:
    job_counts = df1[job_column].value_counts()

    # 시각화 - 직업별 분포
    plt.figure(figsize=(12, 6))
    sns.barplot(x=job_counts.index, y=job_counts.values, palette="viridis")
    plt.title(f"Distribution of {job_column} in Aegir_G1_Normal", fontsize=16)
    plt.xlabel("Class (spec)", fontsize=14)
    plt.ylabel("Count", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
else:
    print(f"The column '{job_column}' is not found in Aegir_G1_Normal.parquet. Check the column names.")