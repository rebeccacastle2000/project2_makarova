import json
from pathlib import Path
from src.primitive_db.constants import META_FILE, DATA_DIR

DATA_PATH = Path(DATA_DIR)
DATA_PATH.mkdir(exist_ok=True)


def load_metadata(filepath=META_FILE):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(data, filepath=META_FILE):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_table_data(table_name):
    filepath = DATA_PATH / f"{table_name}.json"
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name, data):
    filepath = DATA_PATH / f"{table_name}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
