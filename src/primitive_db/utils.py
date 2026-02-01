import json
from pathlib import Path

DB_META_FILE = "db_meta.json"


def load_metadata(filepath=DB_META_FILE):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(data, filepath=DB_META_FILE):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)