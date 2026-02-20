"""Parse extracted text/JSON into structured ski competition result records.

Handles both raw text (from PyMuPDF) and VLM-extracted JSON.
Uses Bedrock Qwen3 VL (text-only mode) to parse unstructured text into JSON.
"""

import json
import re
from extractor import call_qwen3_text

TEXT_PARSE_PROMPT = """请将以下高山滑雪比赛成绩单的原始文本解析为结构化JSON。

要求输出格式（只输出JSON，不要任何其他文字）：
{{
  "competition": "比赛名称",
  "date": "YYYY-MM-DD或原始日期格式",
  "venue": "比赛场地",
  "discipline": "大回转/回转/超级大回转/滑降",
  "gender": "男/女/混合",
  "age_group": "年龄组别，如U11/U12/成人/少年甲组/丁组等",
  "round_type": "总成绩/预赛/决赛/正式成绩/非正式总成绩",
  "results": [
    {{
      "rank": 1,
      "bib": "号码",
      "name": "姓名",
      "team": "代表队/单位",
      "run1_time": "第一次成绩（保持原始格式）",
      "run2_time": "第二次成绩（保持原始格式）",
      "total_time": "总成绩（保持原始格式）",
      "time_diff": "差距/差值（保持原始格式）",
      "status": "OK/DNF/DNS/DQ"
    }}
  ]
}}

注意事项：
1. rank为null表示DNF/DNS/DQ选手
2. 如果选手只有一次成绩没有总成绩，run2_time和total_time设为null
3. status字段：正常完赛为"OK"，未完成为"DNF"，未出发为"DNS"，取消资格为"DQ"
4. 保持所有时间的原始格式，不要转换
5. 只输出JSON，不要输出任何其他内容
6. 如果有多页数据，合并到同一个results数组中

以下是原始文本：

{raw_text}"""


def _extract_json(text: str) -> dict:
    """Extract JSON from text that may contain markdown fences or extra content."""
    text = text.strip()

    # Remove thinking tags if present (Qwen3 sometimes adds these)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # Try to find JSON in markdown code blocks
    match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if match:
        text = match.group(1).strip()

    # Try to find a JSON object directly
    if not text.startswith("{"):
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            text = match.group(0)

    return json.loads(text)


def _merge_results(pages: list) -> dict:
    """Merge parsed results from multiple pages into a single record.

    Uses metadata from the first page, combines all results.
    """
    if not pages:
        return {}
    if len(pages) == 1:
        return pages[0]

    merged = dict(pages[0])
    all_results = []
    seen_bibs = set()

    for page in pages:
        for r in page.get("results", []):
            bib = r.get("bib")
            if bib and bib not in seen_bibs:
                seen_bibs.add(bib)
                all_results.append(r)
            elif not bib:
                all_results.append(r)

    merged["results"] = all_results
    return merged


def time_to_seconds(time_str: str) -> float:
    """Convert time string to seconds. Handle formats:
    - "32.40"       -> 32.40
    - "0:00:24.07"  -> 24.07
    - "00:30.90"    -> 30.90
    - "01:03.32"    -> 63.32
    - "1:39.58"     -> 99.58
    - "00:47.17"    -> 47.17
    - "02:13.23"    -> 133.23

    Returns None for invalid/empty strings.
    """
    if not time_str or not isinstance(time_str, str):
        return None

    time_str = time_str.strip()
    if not time_str or time_str in ("DNF", "DNS", "DQ", "-", ""):
        return None

    try:
        # Format: H:MM:SS.ff or 0:00:24.07
        match = re.match(r"^(\d+):(\d{2}):(\d{2})\.(\d+)$", time_str)
        if match:
            h, m, s, frac = match.groups()
            return int(h) * 3600 + int(m) * 60 + int(s) + float(f"0.{frac}")

        # Format: MM:SS.ff or 01:03.32
        match = re.match(r"^(\d{1,2}):(\d{2})\.(\d+)$", time_str)
        if match:
            m, s, frac = match.groups()
            return int(m) * 60 + int(s) + float(f"0.{frac}")

        # Format: SS.ff or 32.40
        match = re.match(r"^(\d+)\.(\d+)$", time_str)
        if match:
            return float(time_str)

        return None
    except (ValueError, TypeError):
        return None


def parse_results(raw_text: str, source_file: str = "") -> dict:
    """Parse raw text into structured JSON using Qwen3 VL (text-only mode).

    Args:
        raw_text: Raw extracted text from extractor module.
                  May be plain text (from PyMuPDF) or prefixed with [VLM_EXTRACTED]
                  if it came from the vision model.
        source_file: Optional source filename for context.

    Returns:
        Parsed dict with competition metadata and results list.
    """
    if not raw_text or not raw_text.strip():
        return {"error": "Empty input text", "source_file": source_file}

    is_vlm = raw_text.startswith("[VLM_EXTRACTED]")

    if is_vlm:
        # VLM already returned structured data - try to parse directly
        content = raw_text.replace("[VLM_EXTRACTED]", "", 1).strip()

        # There might be multiple VLM results (multi-page scanned PDF)
        parts = content.split("\n\n")
        parsed_pages = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            try:
                parsed = _extract_json(part)
                parsed_pages.append(parsed)
            except (json.JSONDecodeError, ValueError):
                # If direct parse fails, send to LLM for re-parsing
                prompt = TEXT_PARSE_PROMPT.format(raw_text=part)
                llm_response = call_qwen3_text(prompt)
                try:
                    parsed = _extract_json(llm_response)
                    parsed_pages.append(parsed)
                except (json.JSONDecodeError, ValueError):
                    continue

        if parsed_pages:
            result = _merge_results(parsed_pages)
        else:
            return {"error": "Failed to parse VLM output", "source_file": source_file}
    else:
        # Plain text from PyMuPDF - use LLM to parse
        prompt = TEXT_PARSE_PROMPT.format(raw_text=raw_text)
        llm_response = call_qwen3_text(prompt)
        try:
            result = _extract_json(llm_response)
        except (json.JSONDecodeError, ValueError) as e:
            return {
                "error": f"Failed to parse LLM response: {e}",
                "raw_response": llm_response[:500],
                "source_file": source_file,
            }

    # Post-process: add computed seconds fields and normalize
    result["source_file"] = source_file
    for entry in result.get("results", []):
        entry["run1_seconds"] = time_to_seconds(entry.get("run1_time"))
        entry["run2_seconds"] = time_to_seconds(entry.get("run2_time"))
        entry["total_seconds"] = time_to_seconds(entry.get("total_time"))

        # Normalize status
        status = entry.get("status", "OK")
        if status not in ("OK", "DNF", "DNS", "DQ"):
            entry["status"] = "OK"

        # Set rank to None for non-OK status
        if entry.get("status") != "OK" and entry.get("rank") is not None:
            # Keep rank if explicitly provided even for DNF etc.
            pass

    return result
