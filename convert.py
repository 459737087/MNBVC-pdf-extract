import os
import io
import fitz
import shutil
import hashlib
import pprint
import base64
import argparse

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


def convert_wps_pdf(pdf, text_dict):
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
    return text_dict


def convert_pic_pdf(file_path):
    from PyPDF2 import PdfReader, PdfWriter
    pdf_reader = PdfReader(file_path)
    num_pages = len(pdf_reader.pages)
    # 遍历每一页
    for page_num in range(num_pages):
        # 获取当前页对象
        page_obj = pdf_reader.pages[page_num]
        # 获取当前页中的所有对象
        page_objs = page_obj['/Resources']['/XObject'].get_object()
        # print(page_objs)
        # 遍历每个对象
        for obj_name in page_objs:
            # 判断对象是否为图片
            if page_objs[obj_name]['/Subtype'] == '/Image':
                # 获取图片对象
                img_obj = page_objs[obj_name]
                # 获取图片数据
                img_data = img_obj.get_data()
                # 将图片数据保存为文件
                with open(f"{temporary_path}/{page_num+1}.png",
                          'wb') as img_file:
                    img_file.write(img_data)


def parse_pdf_file(file_path):
    if os.path.exists(temporary_path):
        shutil.rmtree(temporary_path)
    os.makedirs(temporary_path)

    try:
        text_dict = {}
        pdf = fitz.open(file_path)
        assert isinstance(pdf, fitz.Document)
        print(f">> PDF info: PageCount {pdf.page_count}, metadata:")
        pprint.pprint(pdf.metadata)
        #         做了一个判断，属于wps的能进行处理，别的方式处理不了
        pdf_type = pdf.metadata.get('creator')
        if 'WPS' in pdf_type:
            text_dict = convert_wps_pdf(pdf, text_dict)
        else:
            print("这些文件都处理不了 -> ", file_path)
            # convert_pic_pdf(file_path)
        pdf.close()
        return text_dict
    except Exception as e:
        print(e)
        return None


def parse_pic_file(file_path):
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


def calculate_md5(text):
    md5_hash = hashlib.md5()
    md5_hash.update(text.encode('utf-8'))
    return md5_hash.hexdigest()


def read_image(image_path):
    # 打开图像文件
    with open(image_path, 'rb') as image_file:
        # 读取文件内容
        encoded_string = base64.b64encode(image_file.read())

        # 将 base64 编码的二进制数据转换为 ASCII 字符串
        # encoded_string = encoded_string.decode('ascii')
        return encoded_string


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

def convert_img_parquet(dst, img_list):

    pd_file = pd.DataFrame([data])
    table = pa.Table.from_pandas(pd_file)
    writer = pq.ParquetWriter(dst+"img.parquet", table.schema)
    for iamge_dir in img_list:
        binary_data = read_image(iamge_dir)
        data['文件md5'] = ""
        data['页码'] = 0
        data['文本'] = ""
        data['图片'] = binary_data
        data['数据类型'] = "图片"
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
    writer.close()

def is_image_file(filename):
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']
    return any(filename.lower().endswith(ext) for ext in image_extensions)

def visit_directory(src, dst):
    """
    复制并处理src目录到dst目录，包括src下的所有子目录和文件。
    """
    img_list = []
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
            if is_image_file(src_file):
                print(f"{src_file} is an image file.")
                dst_file = src_file.replace(src, dst, 1)
                img_list.append(src_file)

        # 如果需要复制空目录（可选）
        for dir in dirs:
            src_dir = os.path.join(root, dir)
            dst_dir = os.path.join(root.replace(src, dst, 1), dir)
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)

    convert_img_parquet(dst, img_list)


# text_dict = parse_pdf_file("29-古镜奇谈2月染长安/阿狸.pdf")
# file_writing(text_dict, "29-古镜奇谈2月染长安/阿狸.pdf")
def main():
    parser = argparse.ArgumentParser(
        description='Convert a base64 string to an image file.')

    # 添加参数，base64_string是一个必须的参数，--output是一个可选的参数
    parser.add_argument('--source', help='输入路径')
    parser.add_argument('--output', help='输出路径')
    args = parser.parse_args()
    source_directory = args.source
    target_directory = args.output
    print(source_directory, target_directory)
    visit_directory(source_directory, target_directory)


if __name__ == '__main__':
    main()
