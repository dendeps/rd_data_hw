import json
import shutil
from fastavro import json_reader, writer
import os

schema = {
    'doc': 'A sale record',
    'name': 'Sales',
    'namespace': 'store',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'},
    ]
}


def process_files(raw_dir: str, stg_dir: str):

    # Ідемпотентність: очищаємо директорію перед записом
    if os.path.exists(stg_dir):
        shutil.rmtree(stg_dir)
    os.makedirs(stg_dir, exist_ok=True)

    for filename in os.listdir(raw_dir):
        if filename.endswith(".json"):
            INPUT_FILE = os.path.join(raw_dir, filename)
            print(f"📂 Reading file: {INPUT_FILE}")

            with open(INPUT_FILE, 'r') as fo:
                data = json.load(fo)

                OUTPUT_FILE = os.path.join(stg_dir, filename.replace("json", "avro"))
                with open(OUTPUT_FILE, "wb") as out:
                    writer(out, schema, data)
            

            