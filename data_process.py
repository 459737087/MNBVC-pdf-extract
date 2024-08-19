from typing import Dict


class mmblock:

    def __init__(self, **kwargs) -> None:
        self.entity_id = kwargs.get("entity_id")
        self.block_id = kwargs.get("block_id")
        self.timestamp = kwargs.get("timestamp")
        self.metadata = kwargs.get("metadata")
        self.text = kwargs.get("text")
        self.image = kwargs.get("image")
        self.ocr_text = kwargs.get("ocr_text")
        self.audio = kwargs.get("audio")
        self.stt_text = kwargs.get("stt_text")
        # self.other_block = kwargs.get("other_block") # ]
        self.block_type = kwargs.get("block_type")
        self.file_md5 = kwargs.get("file_md5")
        self.page_id = kwargs.get("page_id")

    def to_pydict(self) -> Dict:
        return {
            "实体ID": self.entity_id,
            "块ID": self.block_id,
            "时间": self.timestamp,
            "扩展字段": self.metadata,
            "文本": self.text,
            "图片": self.image,
            "OCR文本": self.ocr_text,
            "音频": self.audio,
            "STT文本": self.stt_text,
            "块类型": self.block_type,
            "文件md5": self.file_md5,
            "页ID": self.page_id,
        }
