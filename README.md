# MNBVC-pdf-extract


` python convert.py --source 18\ 逐梦演艺圈（9人半开放）/ --output result.parquet --filter filter.txt`

source是准备处理的文件夹
output是输出的parquet文件
filter是准备过滤掉的文本

安装步骤
```
  conda create --name myenv python=3.10

  安装apt-get install antiword，可以读取doc文件
  pip install -r requirements
```

parquet里面包含的文件支持
文档：doc,docx
图片：jpg,png, jpeg,gif,bmp,tiff,webp
音频：mp3,wav

parquet格式
```
    entity_id=file_id,
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
    page_id=1
```
file_id是为了做区分，保存的音频数据在metadata中，stt_wt中的text字段是语音转文字的内容，timestamp是二维时间戳列表，一个字对应一个时间戳的起始时间到截止时间。
audio里面是语音数据转换成byte
stt_text是语音转文字的内容
