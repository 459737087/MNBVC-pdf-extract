import os
import io
import fitz
import shutil
import hashlib
import magic

import pandas as pd
from PIL import Image
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

temporary_path = "image_temporary"
data = {
    '文件md5': "",
    '文件id': 1,
    '页码': 1,
    '块id': 1,
    '文本': "",
    '图片': b'',
    '时间': "",
    '数据类型': "",
    'bounding box': [],
    '额外信息': ""
}


def parse_pdf_file(file_path):
    if os.path.exists(temporary_path):
        shutil.rmtree(temporary_path)
    os.makedirs(temporary_path)
    try:
        text_dict = {}
        pdf = fitz.open(file_path)
        print("file path is ---> ", file_path)
        # 遍历每一页
        for page_number in range(len(pdf)):
            # 获取页面
            page = pdf[page_number]
            text = page.get_text()
            text_dict[page_number] = text

            zoom_x = 1.0  # 水平缩放比例
            zoom_y = 1.0  # 垂直缩放比例
            mat = fitz.Matrix(zoom_x, zoom_y)  # 创建缩放矩阵
            pix = page.get_pixmap(matrix=mat)  # 使用矩阵渲染页面为像素图
            image_filename = f"{temporary_path}/{page_number+1}.png"
            pix.save(image_filename)
        pdf.close()
        return text_dict
    except Exception as e:
        return None


def calculate_md5(text):
    md5_hash = hashlib.md5()
    md5_hash.update(text.encode('utf-8'))
    return md5_hash.hexdigest()


def read_image(image_path):
    # 打开图像文件
    with open(image_path, 'rb') as file:
        image = Image.open(file)
        # 将图像转换为二进制格式
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format=image.format)
        img_byte_arr = img_byte_arr.getvalue()
    return img_byte_arr


def file_writing(pdf_content_dict, dir):
    output_name = dir.replace("pdf", "parquet")
    image_files = os.listdir(temporary_path)
    pd_file = pd.DataFrame([data])
    table = pa.Table.from_pandas(pd_file)
    writer = pq.ParquetWriter(output_name, table.schema)

    for image_num in range(len(image_files)):
        iamge_dir = temporary_path + '/' + image_files[image_num]
        text_content = pdf_content_dict[image_num]
        now_date = datetime.now()
        text_md5 = calculate_md5(text_content)

        binary_data = read_image(iamge_dir)
        data['文件md5'] = text_md5
        data['页码'] = image_num
        data['文本'] = text_content
        data['图片'] = binary_data
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
    writer.close()


def visit_directory(src, dst):
    """
    复制并处理src目录到dst目录，包括src下的所有子目录和文件。
    """
    # 遍历源目录
    for root, dirs, files in os.walk(src):
        # 计算目标目录路径
        dst_dir = root.replace(src, dst, 1)

        # 如果目标目录不存在，创建它
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)

        # 对每个文件执行处理然后保存到新目录
        for file in files:
            src_file = os.path.join(root, file)

            print(dst_dir, '', src_file)
            #dst_file = os.path.join(dst_dir, file)
            if src_file.endswith('.pdf'):
                pdf_content_dict = parse_pdf_file(src_file)
                if pdf_content_dict is None:
                    continue
                dst_file = src_file.replace(src, dst, 1)
                file_writing(pdf_content_dict, dst_file)

        # 如果需要复制空目录（可选）
        for dir in dirs:
            src_dir = os.path.join(root, dir)
            dst_dir = os.path.join(root.replace(src, dst, 1), dir)
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)


# text_dict = parse_pdf_file("29-古镜奇谈2月染长安/pdf/阿狸.pdf")
# file_writing(text_dict, "29-古镜奇谈2月染长安/pdf/阿狸.pdf")

visit_directory('29-古镜奇谈2月染长安/', 'result/')
