import os
import io
import fitz
from tqdm import tqdm
import shutil
import hashlib
import pprint
import base64
import argparse
import docx
import random
from pathlib import Path
import pandas as pd
import PIL
from PIL import Image
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from docx import Document
from convert_audio import is_speech_in_audio,audio_to_text, audio_to_array
import subprocess
from data_process import mmblock
import numpy as np
import pydub
from pydub import AudioSegment
 
Image.MAX_IMAGE_PIXELS = None
temporary_path = "image_temporary"
data = {
    '实体ID': "",
    '块ID': 1,
    '时间': "",
    '扩展字段': {
        "path": "",
        "Format": "",
        "frame_rate": 0,
        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
    },
    '文本': "",
    '图片': b'',
    'OCR文本': '',
    '音频': np.array([], dtype=np.int16),
    'STT文本': "",
    '块类型': "",
    '文件md5': "",
    '页ID': 1
}
 
 
def read_doc_with_antiword(file_path):
    # 使用antiword命令行工具将.doc文件转换为文本
    result = subprocess.run(['antiword', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if result.returncode == 0:
        # 如果转换成功，则返回文本内容
        return result.stdout.decode('utf-8')
    else:
        # 如果转换失败，则返回错误信息
        return result.stderr.decode('utf-8')
    
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
    formatted_time = currentDateAndTime.strftime("%Y%m%d")
    file_md5 = calculate_md5(file_path)
    block = mmblock(entity_id=file_id,
                    block_id=1,
                    timestamp=formatted_time,
                    metadata={
                        "Format": pdf_type,
                        "path": file_path,
                        "frame_rate": 0,
                        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                    },
                    text="",
                    image=b"",
                    ocr_text="",
                    audio=np.array([], dtype=np.int16),
                    stt_text="",
                    block_type="文本",
                    file_md5=file_md5,
                    page_id=1)
    # 获取字典形式的数据并打印
    data = block.to_pydict()
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)
 
 
def save_unprocess_other(writer, file_path, file_id):
    currentDateAndTime = datetime.now()
    formatted_time = currentDateAndTime.strftime("%Y%m%d")
    file_md5 = calculate_md5(file_path)
    block = mmblock(entity_id=file_id,
                    block_id=1,
                    timestamp=formatted_time,
                    metadata={
                        "Format": "unknown",
                        "path": file_path,
                        "frame_rate": 0,
                        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                    },
                    text="",
                    image=b"",
                    ocr_text="",
                    audio=np.array([], dtype=np.int16),
                    stt_text="",
                    block_type="unknown",
                    file_md5=file_md5,
                    page_id=1)
    # 获取字典形式的数据并打印
    data = block.to_pydict()
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)
 
 
def parse_pic_file(file_path):
    text_dict = {}
    pdf = fitz.open(file_path)
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
    try:
        # 打开图像文件
        with open(image_path, 'rb') as image_file:
            # 读取文件内容
            image = Image.open(image_file)
            # 将图像转换为二进制格式
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format=image.format)
            img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr
    except PIL.UnidentifiedImageError:
        return False
    except  OSError:
        return False
    
 
 
def file_writing(writer, pdf_content_dict, src_file, file_id):
 
    # output_name = dir.replace("pdf", "parquet")
    image_files = os.listdir(temporary_path)
    # pd_file = pd.DataFrame([data])
    # table = pa.Table.from_pandas(pd_file)
    # writer = pq.ParquetWriter(output_name, table.schema)
    if not pdf_content_dict:
        return
    text_md5 = calculate_md5(pdf_content_dict[0])
    currentDateAndTime = datetime.now()
    formatted_time = currentDateAndTime.strftime("%Y%m%d")
    for image_num in range(len(image_files)):
        iamge_dir = temporary_path + '/' + image_files[image_num]
        text_content = pdf_content_dict[image_num]
        now_date = datetime.now()
        block = mmblock(entity_id=file_id,
                        block_id=image_num,
                        timestamp=formatted_time,
                        metadata={
                            "Format": "WPS",
                            "path": src_file,
                            "frame_rate": 0,
                            "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                        },
                        text=text_content,
                        image=b"",
                        ocr_text="",
                        audio=np.array([], dtype=np.int16),
                        stt_text="",
                        block_type="文本",
                        file_md5=text_md5,
                        page_id=1)
 
        # 获取字典形式的数据并打印
        block_dict = block.to_pydict()
        pd_file = pd.DataFrame([block_dict])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
 
 
    # writer.close()
def txt_file_writing(writer, pdf_content_dict, src_file, file_id):
    currentDateAndTime = datetime.now()
    formatted_time = currentDateAndTime.strftime("%Y%m%d")
    text_content = ""
 
    with open(src_file, "r", encoding="utf-8", errors='ignore') as f:
        text_content = f.read()
    text_md5 = calculate_md5(src_file)
    block = mmblock(entity_id=file_id,
                    block_id=1,
                    timestamp=formatted_time,
                    metadata={
                        "Format": "txt",
                        "path": src_file,
                        "frame_rate": 0,
                        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                    },
                    text=text_content,
                    image=b"",
                    ocr_text="",
                    audio=np.array([], dtype=np.int16),
                    stt_text="",
                    block_type="文本",
                    file_md5=text_md5,
                    page_id=1)
 
    # 获取字典形式的数据并打印
    data = block.to_pydict()
 
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)
 
 
def video_file_writing():
    currentDateAndTime = datetime.now()
    formatted_time = currentDateAndTime.strftime("%Y%m%d")
    text_content = ""
 
    with open(src_file, "r", encoding="utf-8", errors='ignore') as f:
        text_content = f.read()
    text_md5 = calculate_md5(src_file)
    block = mmblock(entity_id=file_id,
                    block_id=1,
                    timestamp=formatted_time,
                    metadata={
                        "Format": "txt",
                        "path": src_file,
                        "frame_rate": 0,
                        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                    },
                    text=text_content,
                    image=b"",
                    ocr_text="",
                    audio=np.array([], dtype=np.int16),
                    stt_text="",
                    block_type="文本",
                    file_md5=text_md5,
                    page_id=1)
 
    # 获取字典形式的数据并打印
    data = block.to_pydict()
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)
 
 
def docx_file_writing(writer, pdf_content_dict, file, file_id):
    try:
        currentDateAndTime = datetime.now()
        formatted_time = currentDateAndTime.strftime("%Y%m%d")
        text_content = read_docx(file)
        text_md5 = calculate_md5(file)
        block = mmblock(entity_id=file_id,
                        block_id=1,
                        timestamp=formatted_time,
                        metadata={
                            "Format": "docx",
                            "path": file,
                            "frame_rate": 0,
                            "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                        },
                        text=text_content,
                        image=b"",
                        ocr_text="",
                        audio=np.array([], dtype=np.int16),
                        stt_text="",
                        block_type="文本",
                        file_md5=text_md5,
                        page_id=1)
 
        data = block.to_pydict()
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
    except docx.opc.exceptions.PackageNotFoundError:
        return
 
def doc_file_writing(writer, doc_content, file, file_id):
    try:
        currentDateAndTime = datetime.now()
        formatted_time = currentDateAndTime.strftime("%Y%m%d")
        text_content = str(doc_content)
        text_md5 = calculate_md5(file)
        block = mmblock(entity_id=file_id,
                        block_id=1,
                        timestamp=formatted_time,
                        metadata={
                            "Format": "doc",
                            "path": file,
                            "frame_rate": 0,
                            "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                        },
                        text=text_content,
                        image=b"",
                        ocr_text="",
                        audio=np.array([], dtype=np.int16),
                        stt_text="",
                        block_type="文本",
                        file_md5=text_md5,
                        page_id=1)
 
        data = block.to_pydict()
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
    except docx.opc.exceptions.PackageNotFoundError:
        return
def audio_file_writing(writer, pdf_content_dict, file, file_id):
    import soundfile as sf
    try:
        
        audio_data, frame_rate = audio_to_array(file)
        if "医生与修玛对话录音.m4a" in file:
            return
        if (is_speech_in_audio(file)):
            text, timestamp = audio_to_text(file)
        else:
            print(f"NO person speech detected from audio: {file}")
            text, timestamp = '', None
        currentDateAndTime = datetime.now()
        formatted_time = currentDateAndTime.strftime("%Y%m%d")
        file_md5 = calculate_md5(file)
        
        text_list = []
        text_list.append(text)
        if len(text) ==0:
            text_array = np.array([""], dtype=np.chararray)
        else:
            text_array = np.array(text_list, dtype=np.chararray)
        if timestamp is None:
            timestamp = [[0],[0]]
        block = mmblock(entity_id=file_id,
                        block_id=1,
                        timestamp=formatted_time,
                        metadata={
                            "Format": "mp3",
                            "path": file,
                            "frame_rate": frame_rate,
                            "stt_wt": {"text": text_array,"stt_ts":timestamp}
                        },
                        text="",
                        image=b"",
                        ocr_text="",
                        audio=audio_data,
                        stt_text=text,
                        block_type="音频",
                        file_md5=file_md5,
                        page_id=1)
 
        data = block.to_pydict()
        pd_file = pd.DataFrame([data])
        info_table = pa.Table.from_pandas(pd_file)
        writer.write_table(info_table)
    except ValueError:
        print("eror ---------")
    except pydub.exceptions.CouldntDecodeError:
        print("error pydub.exceptions.CouldntDecodeError", file)
    except sf.LibsndfileError:
        print("error sound is", file)
    except IndexError:
        print("IndexError:")
 
def convert_img_parquet(writer, iamge_dir, file_id):
 
    currentDateAndTime = datetime.now()
    formatted_time = currentDateAndTime.strftime("%Y%m%d")
    binary_data = read_image(iamge_dir)
    if binary_data == False:
        return
    img_md5 = calculate_md5(iamge_dir)
    block = mmblock(entity_id=file_id,
                    block_id=1,
                    timestamp=formatted_time,
                    metadata={
                        "Format": "jpg",
                        "path": iamge_dir,
                        "frame_rate": 0,
                        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                    },
                    text="",
                    image=binary_data,
                    ocr_text="",
                    audio=np.array([], dtype=np.int16),
                    stt_text="",
                    block_type="图片",
                    file_md5=img_md5,
                    page_id=1)
 
    # 获取字典形式的数据并打印
    data = block.to_pydict()
    pd_file = pd.DataFrame([data])
    info_table = pa.Table.from_pandas(pd_file)
    writer.write_table(info_table)
    # writer.close()
 
 
def process_list(file_list, writer, name_list):
    count = 0
    
    for file in tqdm(file_list):
        file = str(file)
        file_id = ""
        for key, value in name_list.items():
            if key in file:
                file_id = key
        if len(str(file_id)) == 0:
            print("there is not file in ", file)
            continue
        lower_file = file.lower()
        pdf_content_dict= {}
        if lower_file.endswith('.pdf'):
            pdf_content_dict = parse_pdf_file(file, writer, file_id)
            if pdf_content_dict is None:
                continue
            file_writing(writer, pdf_content_dict, file, file_id)
        elif lower_file.endswith(".txt"):
            txt_file_writing(writer, pdf_content_dict, file, file_id)
        elif lower_file.endswith(".docx"):
            docx_file_writing(writer, pdf_content_dict, file, file_id)
        elif lower_file.endswith(".mp3") or lower_file.endswith(".wav") or  lower_file.endswith(".m4a"):
            audio_file_writing(writer, pdf_content_dict, file, file_id)
        else:
            if file.endswith(".doc") or  file.endswith(".DOC"):
                doc_content = read_doc_with_antiword(file)
                doc_file_writing(writer, doc_content, file, file_id)
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
 
 
def visit_directory(src, dst, filter_file):
    block = mmblock(entity_id="",
                    block_id=1,
                    timestamp="",
                    metadata={
                        "Format": "",
                        "path": "value2",
                        "frame_rate": 0,
                        "stt_wt": {"text": np.array([""], dtype=np.chararray),"stt_ts": [[0],[0]]}
                    },
                    text="",
                    image=b"",
                    ocr_text="",
                    audio=np.array([], dtype=np.int16),
                    stt_text="",
                    block_type="",
                    file_md5="",
                    page_id=1)
    filter_list = []
    if filter_file is not None:
        with open(filter_file, 'r') as file:
            for line in file:
                # 去除行尾的换行符
                line = line.strip()
                # 处理每一行
                filter_list.append(line)
    # 获取字典形式的数据并打印
    block_dict = block.to_pydict()
    pd_file = pd.DataFrame([block_dict])
    table = pa.Table.from_pandas(pd_file)
    # writer = pq.ParquetWriter(dst, table.schema)
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
            flag =False
            src_file = os.path.join(root, file)
            for filter_filename  in filter_list:
                if filter_filename in file:
                    flag = True
            if flag:
                continue
 
            src_file = Path(str(src_file))
            file_list.append(src_file)
 
    file_list.sort()
    
    current_size = 0
    batch_files = []
    batch_index = 1
    max_size_bytes = 30 * 1024 * 1024*1024
    print("file_list is ",len(file_list))
    for file_path in tqdm(file_list, desc='Processing audio files'):
        file_size = os.path.getsize(file_path)
 
        if current_size + file_size > max_size_bytes:
            
            output_file = f"{dst}/output_part_{batch_index}.parquet"
            
            writer = pq.ParquetWriter(output_file, table.schema)
            process_list(batch_files, writer, name_list)
            batch_index += 1
 
            current_size = 0
            batch_files = []
        
        batch_files.append(file_path)
        current_size += file_size
    output_file = f"{dst}/output_part_{batch_index}.parquet"
 
    writer = pq.ParquetWriter(output_file, table.schema)
    process_list(batch_files, writer, name_list)
    batch_index += 1
 
    current_size = 0
    batch_files = []
    writer.close()
 
 
def main():
    parser = argparse.ArgumentParser(
        description='Convert a base64 string to an image file.')
 
    # 添加参数，base64_string是一个必须的参数，--output是一个可选的参数
    parser.add_argument('--source', help='输入路径')
    parser.add_argument('--output', help='输出文件夹名称')
    parser.add_argument('--filter', help="筛掉的文件名字")
    args = parser.parse_args()
    source_directory = args.source
    target_directory = args.output
    filter_file = args.filter
    
    visit_directory(source_directory, target_directory, filter_file)
 
 
if __name__ == '__main__':
    main()
