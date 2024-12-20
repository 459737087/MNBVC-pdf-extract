import os
from pydub import AudioSegment
from funasr import AutoModel
from funasr.utils.postprocess_utils import rich_transcription_postprocess
from silero_vad import load_silero_vad, read_audio, get_speech_timestamps
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import hashlib
import time
import uuid


def audio_to_array(file_path):
    """将音频文件转换为numpy数组"""
    audio = AudioSegment.from_file(file_path)
    data = np.array(audio.get_array_of_samples())
    return data, audio.frame_rate

def load_model():
    model = AutoModel(
        model="iic/speech_paraformer-large-vad-punc_asr_nat-zh-cn-16k-common-vocab8404-pytorch",
        punc_model="ct-punc",
        vad_model="fsmn-vad",
        vad_kwargs={"max_single_segment_time": 30000},
        device="cuda:0"
    )
    return model

def is_speech_in_audio(file_path):
    """
    检测音频是否包含人类语音
    参数:
    file_path (str): 音频文件的路径
    返回:
    bool: 如果音频包含语音，返回 True；否则返回 False
    """
    model = load_silero_vad()
    wav = read_audio(file_path)
    speech_timestamps = get_speech_timestamps(
        wav,
        model,
        return_seconds=True,  # Return speech timestamps in seconds (default is samples)
    )
    return len(speech_timestamps) > 0


def audio_to_text(file_path):
    """将音频文件解析为文本"""
    model = load_model()
    time1 = time.time()
    ret_dict = model.generate(
        input=file_path,
        cache={},
        language="auto",
        use_itn=True,
        batch_size_s=60,
        merge_vad=True,  #
        merge_length_s=15,
    )[0]
    time2 = time.time()
    print(f'-- cost : {time2 - time1:.2f} seconds')  
    text = rich_transcription_postprocess(ret_dict['text'])
    timestamp = ret_dict['timestamp']

    return text, timestamp


def get_file_md5(file_path):
    """计算文件的MD5值"""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()


def create_dataframe(file_paths):
    """创建包含音频数据和元数据的DataFrame"""
    data_list = []
    for file_path in tqdm(file_paths, desc='Processing audio files'):
        file_md5 = get_file_md5(file_path)
        file_name = os.path.basename(file_path)
        file_id = str(uuid.uuid4())
        processing_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        data_type = '音频'
        if (is_speech_in_audio(file_path)):
            text, timestamp = audio_to_text(file_path)
        else:
            print(f"NO person speech detected from audio: {file_path}")
            text, timestamp = '', None
        extra_info = {
            'duration': AudioSegment.from_file(file_path).duration_seconds,
            'timestamp': timestamp
        }
        data, frame_rate = audio_to_array(file_path)
        data_list.append({
            'file_md5': file_md5,
            'file_name': file_name,
            'file_id': file_id,
            'processing_time': processing_time,
            'data_type': data_type,
            'audio_data': data,
            'audio_text': text,
            'frame_rate': frame_rate,
            'extra_info': extra_info
        })
    df = pd.DataFrame(data_list)
    return df


def save_to_parquet(df, output_file):
    """将DataFrame保存为Parquet文件，并转换字段为中文"""
    columns_mapping = {
        'file_md5': '文件md5',
        'file_name': '文件名',
        'file_id': '文件id',
        'processing_time': '处理时间',
        'data_type': '数据类型',
        'audio_data': '音频数据',
        'audio_text': '文本',
        'frame_rate': '采样率',
        'extra_info': '额外信息'
    }
    # 重命名DataFrame的列
    df.rename(columns=columns_mapping, inplace=True)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)


def process_audio_files_in_batches_by_size(directory, max_size_mb, output_prefix):
    """处理目录下的所有音频文件并根据大小分批保存为Parquet文件"""
    file_paths = [
        os.path.join(directory, f) for f in os.listdir(directory)
        if f.endswith(('.mp3', '.wav'))
    ]

    current_size = 0
    batch_files = []
    batch_index = 1
    max_size_bytes = max_size_mb * 1024 * 1024

    for file_path in tqdm(file_paths, desc='Processing audio files'):
        file_size = os.path.getsize(file_path)

        if current_size + file_size > max_size_bytes:
            df = create_dataframe(batch_files)
            output_file = f"{output_prefix}_part_{batch_index}.parquet"
            save_to_parquet(df, output_file)
            print(f"Saved batch {batch_index} to {output_file}")
            batch_index += 1

            current_size = 0
            batch_files = []

        batch_files.append(file_path)
        current_size += file_size

    if batch_files:
        df = create_dataframe(batch_files)
        output_file = f"{output_prefix}_part_{batch_index}.parquet"
        save_to_parquet(df, output_file)
        print(f"Saved batch {batch_index} to {output_file}")


directory = './'
max_size_mb = 500  # 每个Parquet文件的最大大小，单位为MB
output_prefix = 'output/audio'

process_audio_files_in_batches_by_size(directory, max_size_mb, output_prefix)
