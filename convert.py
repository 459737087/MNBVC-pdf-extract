import os
import io
import fitz
import shutil
import hashlib
import pprint
import base64
import argparse
import docx
import random
from pathlib import Path
import pandas as pd
from PIL import Image
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from docx import Document
from convert_audio import audio_to_array

import numpy as np

temporary_path = "image_temporary"
data = {
    '文件md5': "",
    '文件id': "",
    '页码': 1,
    '块id': 1,
    '文本': "",
    '图片': b'',
    '音频数据': np.array([], dtype=np.int16),
    '时间': "",
    '数据类型': "",
    'bounding box': [],
    '额外信息': {
        "path": "",
        "Format": "",
        "frame_rate": 0
    }
}


def generate_18_digits():
    digits = []
    for _ in range(18):
        digits.append(str(random.randint(0, 9)))
    return int(''.join(digits))


def read_docx(file_path):
    doc = Document(file_path)
    content = []
    for para in doc.paragraphs:
        content.append(para.text)
    return '\n'.join(content)


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


def parse_pdf_file(file_path, writer, file_id):
    if os.path.exists(temporary_path):
        shutil.rmtree(temporary_path)
    os.makedirs(temporary_path)

    try:
        text_dict = {}
        pdf = fitz.open(file_path)
        assert isinstance(pdf, fitz.Document)
        # print(f">> PDF info: PageCount {pdf.page_count}, metadata:")
        # pprint.pprint(pdf.metadata)
        #         做了一个判断，属于wps的能进行处理，别的方式处理不了
        pdf_type = pdf.metadata.get('creator')
        if 'WPS' in pdf_type:
            text_dict = convert_wps_pdf(pdf, text_dict)
        else:
            # print("这些文件都处理不了 -> ", file_path, pdf_type)
            save_unprocess(writer, file_path, pdf_type, file_id)
            # convert_pic_pdf(file_path)
        pdf.close()
        return text_dict
    except Exception as e:
        print(e)
        return None


def save_unprocess(writer, file_path, pdf_type, file_id):
    currentDateAndTime = datetime.now()
    file_md5 = calculate_md5(file_path)
    data['文件md5'] = file_md5
    data['页码'] = 1
    data['文件id'] = file_id
    data['文本'] = ""
    data['时间'] = str(currentDateAndTime)
    data['图片'] = b""
    data['数据类型'] = "文本"
    data["额外信息"]["Format"] = pdf_type
    data["额外信息"]["path"] = file_path
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)


def parse_pic_file(file_path):
    text_dict = {}
    pdf = fitz.open(file_path)
    # print("file path is ---> ", file_path)
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


def file_writing(writer, pdf_content_dict, src_file, file_id):

    # output_name = dir.replace("pdf", "parquet")
    image_files = os.listdir(temporary_path)
    # pd_file = pd.DataFrame([data])
    # table = pa.Table.from_pandas(pd_file)
    # writer = pq.ParquetWriter(output_name, table.schema)
    if not pdf_content_dict:
        return
    # print("============", image_files, pdf_content_dict[0])
    text_md5 = calculate_md5(pdf_content_dict[0])
    currentDateAndTime = datetime.now()
    for image_num in range(len(image_files)):
        iamge_dir = temporary_path + '/' + image_files[image_num]
        text_content = pdf_content_dict[image_num]
        now_date = datetime.now()

        # binary_data = read_image(iamge_dir)
        data['文件md5'] = text_md5
        data['页码'] = image_num + 1
        data['文本'] = text_content
        data['文件id'] = file_id
        data['图片'] = b""
        data['时间'] = str(currentDateAndTime)
        data['数据类型'] = "文本"
        data["额外信息"]["Format"] = "WPS"
        data["额外信息"]["path"] = src_file
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)


    # writer.close()
def txt_file_writing(writer, pdf_content_dict, src_file, file_id):
    currentDateAndTime = datetime.now()
    text_content = ""

    with open(src_file, "r", encoding="utf-8", errors='ignore') as f:
        text_content = f.read()
    text_md5 = calculate_md5(src_file)
    data['文件md5'] = text_md5
    data['页码'] = 1
    data['文本'] = text_content
    data['文件id'] = file_id
    data['图片'] = b""
    data['时间'] = str(currentDateAndTime)
    data['数据类型'] = "文本"
    data["额外信息"]["Format"] = "txt"
    data["额外信息"]["path"] = src_file
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)


def video_file_writing():
    currentDateAndTime = datetime.now()
    text_content = ""

    with open(src_file, "r", encoding="utf-8", errors='ignore') as f:
        text_content = f.read()
    text_md5 = calculate_md5(src_file)
    data['文件md5'] = text_md5
    data['页码'] = 1
    data['文本'] = text_content
    data['文件id'] = file_id
    data['图片'] = b""
    data['时间'] = str(currentDateAndTime)
    data['数据类型'] = "文本"
    data["额外信息"]["Format"] = "txt"
    data["额外信息"]["path"] = src_file
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)


def doc_file_writing(writer, pdf_content_dict, file, file_id):
    try:
        currentDateAndTime = datetime.now()
        text_content = read_docx(file)

        text_md5 = calculate_md5(file)
        data['文件md5'] = text_md5
        data['页码'] = 1
        data['文本'] = text_content
        data['文件id'] = file_id
        data['图片'] = b""
        data['时间'] = str(currentDateAndTime)
        data['数据类型'] = "文本"
        data["额外信息"]["Format"] = "doc"
        data["额外信息"]["path"] = file
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
    except docx.opc.exceptions.PackageNotFoundError:
        return


def audio_file_writing(writer, pdf_content_dict, file, file_id):
    audio_data, frame_rate = audio_to_array(file)
    currentDateAndTime = datetime.now()
    file_md5 = calculate_md5(file)
    data['文件md5'] = file_md5
    data['文件id'] = file_id
    data['页码'] = 1
    data['文本'] = ""
    data['图片'] = b''
    data['音频数据'] = audio_data
    data['时间'] = str(currentDateAndTime)
    data['数据类型'] = "音频"
    data["额外信息"]["Format"] = "mp3"
    data["额外信息"]["path"] = file
    data["额外信息"]["frame_rate"] = frame_rate
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)


def convert_img_parquet(writer, iamge_dir, file_id):

    currentDateAndTime = datetime.now()
    binary_data = read_image(iamge_dir)
    img_md5 = calculate_md5(iamge_dir)
    data['文件md5'] = img_md5
    data['文件id'] = file_id
    data['页码'] = 1
    data['文本'] = ""
    data['图片'] = binary_data
    data['时间'] = str(currentDateAndTime)
    data['数据类型'] = "图片"
    data["额外信息"]["Format"] = "jpg"
    data["额外信息"]["path"] = iamge_dir
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)
    # writer.close()


def process_list(file_list, writer, name_list):
    count = 0
    for file in file_list:
        file = str(file)
        file_id = ""
        for key, value in name_list.items():
            if key in file:
                file_id = key
        if len(str(file_id)) == 0:
            print("there is not file in ", file)
            continue
        lower_file = file.lower()
        if lower_file.endswith('.pdf'):
            pdf_content_dict = parse_pdf_file(file, writer, file_id)
            if pdf_content_dict is None:
                continue
            file_writing(writer, pdf_content_dict, file, file_id)
        elif lower_file.endswith(".txt"):
            txt_file_writing(writer, pdf_content_dict, file, file_id)
        elif lower_file.endswith(".docx"):
            doc_file_writing(writer, pdf_content_dict, file, file_id)
        elif lower_file.endswith(".mp3") or lower_file.endswith(".wav"):
            audio_file_writing(writer, pdf_content_dict, file, file_id)
        else:
            if file.endswith(".doc"):
                # print("doc ----------", file)
                # from win32com import client as wc
                # word = wc.Dispatch("Word.Application")
                # doc = word.Documents.Open(file)
                # new_file = "temporary.docx"
                # doc.SaveAs(new_file, 12)
                # doc.Close()
                # word.Quit()
                # doc_file_writing(writer, pdf_content_dict, new_file, file_id)
                count += 1
                continue
            if is_image_file(file):
                convert_img_parquet(writer, file, file_id)
            else:
                print("unrecognized file is ------", file)
    print("total ------------------", count)


def is_image_file(filename):
    image_extensions = [
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'
    ]
    return any(filename.lower().endswith(ext) for ext in image_extensions)


def visit_directory(src, dst):
    pd_file = pd.DataFrame([data])
    table = pa.Table.from_pandas(pd_file)
    writer = pq.ParquetWriter(dst, table.schema)
    name_list = {}
    source_files = os.listdir(src)

    for source_file in source_files:
        file_id = generate_18_digits()
        name_list[source_file] = file_id
    file_list = []
    file_id = 1
    # 遍历源目录
    for root, dirs, files in os.walk(src):
        dst_dir = root.replace(src, dst, 1)
        # 对每个文件执行处理然后保存到新目录
        for file in files:
            src_file = os.path.join(root, file)
            # print(dst_dir, '--------------', src_file)
            src_file = str(src_file)

            src_file = Path(src_file)
            file_list.append(src_file)

    file_list.sort()
    process_list(file_list, writer, name_list)

    writer.close()


def main():
    parser = argparse.ArgumentParser(
        description='Convert a base64 string to an image file.')

    # 添加参数，base64_string是一个必须的参数，--output是一个可选的参数
    parser.add_argument('--source', help='输入路径')
    parser.add_argument('--output', help='输出文件名称，以parquet格式结尾')
    args = parser.parse_args()
    source_directory = args.source
    target_directory = args.output
    # print(source_directory,'------------', target_directory)
    visit_directory(source_directory, target_directory)


if __name__ == '__main__':
    main()
