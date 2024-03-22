import pandas as pd
import pyarrow.parquet as pq
import base64

# 读取 Parquet 文件
df = pd.read_parquet('result/阿狸.parquet')

for index, row in df.iterrows():
    # 这里可以访问 row 中的每个元素，例如 row['column_name']
    print(row)
    base64_string = row['图片']
    image_data = base64.b64decode(base64_string)
    with open('1.jpg', 'wb') as file:
        file.write(image_data)
    break