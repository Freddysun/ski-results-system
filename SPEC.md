# Alpine Skiing Results Query System - Project Specification

## Overview
A web-based system for querying alpine skiing competition results from Chinese skiing competitions (2021-2026). Data is stored as PDF/image files in S3, extracted via LLM (AWS Bedrock Qwen3 VL), stored in SQLite, and queried through a Streamlit UI.

## Architecture (3-Tier)
```
┌─────────────────────────────────┐
│  Presentation: Streamlit (app.py)│
├─────────────────────────────────┤
│  Business Logic:                 │
│  - extractor.py (PDF/Image OCR)  │
│  - parser.py (structured parsing)│
│  - ingestion.py (pipeline)       │
├─────────────────────────────────┤
│  Data: SQLite (ski_results.db)   │
│  - database.py (schema + CRUD)   │
└─────────────────────────────────┘
```

## Data Source
- S3 bucket: `s3://fsun-uswest2/ski/比赛成绩汇总/`
- ~650 files: PDF (474), JPG (111), PNG (58), HEIC (4), XLS (1)
- Organized: `{season}/{competition}/{event_files}`

### Seasons
- 21-22雪季, 22-23雪季, 23-24雪季, 24-25雪季, 25-26雪季
- Plus root-level files (朝阳区锦标赛, 首届北京滑雪公开赛, etc.)

### File naming patterns
- `高山滑雪大回转_U11_女子.pdf` (event_agegroup_gender)
- `11.非正式总成绩_高山滑雪大回转_丁组_女子.pdf`
- `出发顺序/21.出发顺序_高山滑雪大回转_U11_女子.pdf` (start order, not results)
- Some are large combined books: `滑雪 成绩册 冠军赛.pdf` (13MB)

### Key filter: Only process "成绩" (results) files, skip "出发顺序" (start order) and "秩序册" (order book)

## Technical Environment
- Python 3.9, Node 18
- Packages: streamlit 1.44.1, boto3 1.37.35, PyMuPDF 1.23.7, pillow 11.2.1, pandas, sqlalchemy
- AWS Bedrock model: `qwen.qwen3-vl-235b-a22b` (us-west-2)
- API: invoke_model with OpenAI-compatible format
- Images: base64 encoded (S3 URIs NOT supported)

## Database Schema (SQLite)

### Table: competitions
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| season | TEXT | e.g., "25-26雪季" |
| name | TEXT | e.g., "2025北京市U系列冠军赛" |
| venue | TEXT | e.g., "密苑云顶乐园" |
| date | TEXT | e.g., "2025-01-15" |
| organizer | TEXT | Optional |

### Table: events
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| competition_id | INTEGER FK | → competitions.id |
| discipline | TEXT | 回转/大回转/超级大回转/滑降 |
| gender | TEXT | 男/女/混合 |
| age_group | TEXT | U10/U11/U12/U13/U14/U15/U18/U20/少年甲组/少年乙组/青年组/成人/丁组/丙组/乙组/甲组 |
| round_type | TEXT | 预赛/决赛/总成绩 |
| source_file | TEXT | S3 key of source file |

### Table: results
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| event_id | INTEGER FK | → events.id |
| rank | INTEGER | NULL if DNF/DNS/DQ |
| bib | TEXT | Bib number |
| name | TEXT | Athlete name |
| team | TEXT | Team/代表队/单位 |
| run1_time | TEXT | First run time (original format) |
| run2_time | TEXT | Second run time (original format) |
| total_time | TEXT | Total time (original format) |
| run1_seconds | REAL | First run in seconds |
| run2_seconds | REAL | Second run in seconds |
| total_seconds | REAL | Total in seconds |
| time_diff | TEXT | Difference from winner |
| status | TEXT | OK/DNF/DNS/DQ |

### Table: processed_files
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| s3_key | TEXT UNIQUE | S3 file key |
| file_type | TEXT | pdf/jpg/png/heic/xls |
| processed_at | TEXT | ISO timestamp |
| status | TEXT | success/failed/skipped |
| error_message | TEXT | Error details if failed |

## Bedrock Qwen3 VL API Usage

```python
import boto3, json, base64

client = boto3.client('bedrock-runtime', region_name='us-west-2')

def call_qwen3_vl(image_base64: str, prompt: str, media_type: str = "image/png") -> str:
    body = {
        "messages": [
            {"role": "system", "content": [{"type": "text", "text": "You are an expert at reading alpine skiing competition result sheets. Extract data accurately."}]},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{image_base64}"}},
                {"type": "text", "text": prompt}
            ]}
        ],
        "max_tokens": 4096,
        "temperature": 0.1
    }
    response = client.invoke_model(
        modelId='qwen.qwen3-vl-235b-a22b',
        contentType='application/json',
        accept='application/json',
        body=json.dumps(body)
    )
    result = json.loads(response['body'].read())
    return result['choices'][0]['message']['content']
```

## PDF Text Extraction Strategy
1. **Text-based PDFs**: Use PyMuPDF (`fitz`) to extract text directly - fast and free
2. **Scanned/image PDFs**: Convert pages to images using PyMuPDF, then use Bedrock Qwen3 VL
3. **Image files (JPG/PNG/HEIC)**: Use Bedrock Qwen3 VL directly
4. **Decision logic**: If PyMuPDF extracts >50 chars of meaningful text → use text; otherwise → use VLM

## LLM Extraction Prompt (for Qwen3 VL)
The prompt should ask the model to return JSON with this structure:
```json
{
  "competition": "比赛名称",
  "date": "YYYY-MM-DD or original",
  "venue": "场地",
  "discipline": "大回转",
  "gender": "女",
  "age_group": "U11",
  "round_type": "总成绩",
  "results": [
    {
      "rank": 1,
      "bib": "13",
      "name": "姚知涵",
      "team": "顺义区",
      "run1_time": "0:00:24.07",
      "run2_time": "0:00:24.02",
      "total_time": "0:00:48.09",
      "time_diff": "0:00:00.00",
      "status": "OK"
    }
  ]
}
```

## Streamlit UI Requirements
1. **Sidebar filters**: Season, Competition, Discipline, Age Group, Gender, Athlete Name (text search)
2. **Main area**: Results table (sortable), summary statistics
3. **Features**:
   - Full-text search for athlete names
   - Cross-competition history for an athlete
   - Ranking leaderboards by age group
   - Data ingestion status/progress page

## Project Structure
```
/home/ec2-user/ski-results-system/
├── SPEC.md              # This file
├── app.py               # Streamlit main application
├── database.py          # SQLite schema, CRUD operations
├── extractor.py         # PDF/image text extraction (PyMuPDF + Bedrock)
├── parser.py            # Parse extracted text/JSON into DB records
├── ingestion.py         # S3 download + orchestrate extraction + DB insert
├── config.py            # Configuration constants
├── test_system.py       # System tests
├── requirements.txt     # Python dependencies
├── data/
│   ├── ski_results.db   # SQLite database (generated)
│   └── cache/           # Downloaded file cache
└── samples/             # Sample files for development
```
