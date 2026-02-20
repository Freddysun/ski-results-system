import os

S3_BUCKET = "fsun-uswest2"
S3_PREFIX = "ski/比赛成绩汇总/"
AWS_REGION = "us-west-2"
BEDROCK_MODEL_ID = "qwen.qwen3-vl-235b-a22b"
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "ski_results.db")
CACHE_DIR = os.path.join(os.path.dirname(__file__), "data", "cache")
SUPPORTED_EXTENSIONS = [".pdf", ".jpg", ".jpeg", ".png", ".heic"]
SKIP_PATTERNS = ["出发顺序", "秩序册"]
