"""PDF and image text extraction for ski competition results.

Uses PyMuPDF for text-based PDFs and Bedrock Qwen3 VL for scanned/image content.
"""

import fitz  # PyMuPDF
import boto3
import json
import base64
from pathlib import Path

BEDROCK_MODEL_ID = "qwen.qwen3-vl-235b-a22b"
AWS_REGION = "us-west-2"

# Minimum chars from PyMuPDF to consider it a text-based PDF
TEXT_THRESHOLD = 50

MEDIA_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".heic": "image/heic",
}

VLM_EXTRACTION_PROMPT = """请仔细阅读这张高山滑雪比赛成绩单图片，提取所有信息并以JSON格式输出。

要求输出格式（只输出JSON，不要任何其他文字）：
{
  "competition": "比赛名称",
  "date": "YYYY-MM-DD或原始格式",
  "venue": "比赛场地",
  "discipline": "大回转/回转/超级大回转/滑降",
  "gender": "男/女/混合",
  "age_group": "年龄组别，如U11/U12/成人/少年甲组/丁组等",
  "round_type": "总成绩/预赛/决赛/正式成绩/非正式总成绩",
  "results": [
    {
      "rank": 1,
      "bib": "号码",
      "name": "姓名",
      "team": "代表队/单位",
      "run1_time": "第一次成绩（保持原始格式）",
      "run2_time": "第二次成绩（保持原始格式）",
      "total_time": "总成绩（保持原始格式）",
      "time_diff": "差距/差值（保持原始格式）",
      "status": "OK/DNF/DNS/DQ"
    }
  ]
}

注意事项：
1. rank为null表示DNF/DNS/DQ选手
2. 如果选手只有一次成绩没有总成绩，run2_time和total_time设为null
3. status字段：正常完赛为"OK"，未完成为"DNF"，未出发为"DNS"，取消资格为"DQ"
4. 保持所有时间的原始格式，不要转换
5. 只输出JSON，不要输出任何其他内容"""


def call_qwen3_vl(image_bytes: bytes, media_type: str, prompt: str) -> str:
    """Call Bedrock Qwen3 VL for image understanding/OCR."""
    client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    b64_data = base64.b64encode(image_bytes).decode("utf-8")
    body = {
        "messages": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "你是一个专业的高山滑雪比赛成绩单识别专家。请准确提取成绩单中的所有信息。",
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{media_type};base64,{b64_data}"
                        },
                    },
                    {"type": "text", "text": prompt},
                ],
            },
        ],
        "max_tokens": 8192,
        "temperature": 0.1,
    }
    response = client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body),
    )
    result = json.loads(response["body"].read())
    return result["choices"][0]["message"]["content"]


def call_qwen3_text(prompt: str) -> str:
    """Call Bedrock Qwen3 VL in text-only mode (no image)."""
    client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    body = {
        "messages": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "你是一个专业的高山滑雪比赛成绩单识别专家。请准确提取成绩单中的所有信息。",
                    }
                ],
            },
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        ],
        "max_tokens": 8192,
        "temperature": 0.1,
    }
    response = client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body),
    )
    result = json.loads(response["body"].read())
    return result["choices"][0]["message"]["content"]


def extract_from_pdf(file_path: str) -> str:
    """Extract text from PDF. Use PyMuPDF first; fall back to VLM for scanned PDFs.

    Returns raw text for text PDFs, or JSON string from VLM for scanned PDFs.
    """
    doc = fitz.open(file_path)
    all_text = []
    scanned_pages = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text().strip()
        if len(text) > TEXT_THRESHOLD:
            all_text.append(text)
        else:
            scanned_pages.append(page_num)

    # If all pages have good text extraction, return combined text
    if not scanned_pages:
        doc.close()
        return "\n\n".join(all_text)

    # If some pages are scanned, render those to images and use VLM
    vlm_results = []
    for page_num in scanned_pages:
        page = doc[page_num]
        # Render at 2x resolution for better OCR
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img_bytes = pix.tobytes("png")
        result = call_qwen3_vl(img_bytes, "image/png", VLM_EXTRACTION_PROMPT)
        vlm_results.append(result)

    doc.close()

    # If we had a mix of text and scanned pages, combine them
    if all_text and vlm_results:
        combined = "\n\n".join(all_text)
        combined += "\n\n[VLM_EXTRACTED]\n" + "\n\n".join(vlm_results)
        return combined
    elif vlm_results:
        # Pure scanned PDF - return VLM results marked accordingly
        return "[VLM_EXTRACTED]\n" + "\n\n".join(vlm_results)

    return ""


def extract_from_image(file_path: str) -> str:
    """Extract text from image using Bedrock Qwen3 VL.

    Returns the VLM response (expected to be JSON).
    """
    path = Path(file_path)
    suffix = path.suffix.lower()
    media_type = MEDIA_TYPES.get(suffix, "image/jpeg")

    with open(file_path, "rb") as f:
        image_bytes = f.read()

    result = call_qwen3_vl(image_bytes, media_type, VLM_EXTRACTION_PROMPT)
    return "[VLM_EXTRACTED]\n" + result


def extract(file_path: str) -> str:
    """Auto-detect file type and extract text/data.

    Returns raw text or VLM-extracted content prefixed with [VLM_EXTRACTED].
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        return extract_from_pdf(file_path)
    elif suffix in MEDIA_TYPES:
        return extract_from_image(file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")
