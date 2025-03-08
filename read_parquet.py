import pandas as pd
from PIL import Image
import io
import os
# 读取Parquet文件
# df = pd.read_parquet('/input1/6.parquet')
# print("======",df)
# 遍历DataFrame的行
num = 1
token =0 
pic =0
import pyarrow.parquet as pq

path = "/input3/output/"
files = os.listdir(path)

for file in files:
# 打开 Parquet 文件
    filename = path+file
    if not filename.endswith(".parquet"):
        continue
    print(filename)
    parquet_file = pq.ParquetFile(filename)

    # 分块读取
    for batch in parquet_file.iter_batches(batch_size=100):  # 可以根据需要调整 batch_size
        # 在这里处理每个 batch
        df = batch.to_pandas()
        for index, row in df.iterrows():
    #     # 处理图像数据
            print("row ------------------->", row["扩展字段"])
            if "音频" in row["块类型"]:
                print("-------------------")
            token += len(str(row['文本']))
            if row["块类型"]=="图片":
                pic+=1

        # 处理完释放内存
        del df

    
print(pic, token)
