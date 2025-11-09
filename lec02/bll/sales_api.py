import json
import os
import shutil

import requests
from dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str, token: str) -> None:
    # TODO: implement me
    # 1. get data from the API
    # 2. save data to disk

    #URL = 'https://fake-api-vycpfa6oca-uc.a.run.app'
    headers = {
        "Authorization": f"{token}"
    }

    save_path = os.path.join(raw_dir)

    # Ідемпотентність: очищаємо директорію перед записом
    if os.path.exists(save_path):
        shutil.rmtree(save_path)
    os.makedirs(save_path, exist_ok=True)
    
    page = 1

    while True:
        url = f"https://fake-api-vycpfa6oca-uc.a.run.app/sales?date={date}&page={page}"
        try:
            response = requests.get(url, headers=headers)
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            break

        if response.status_code == 404:
            # Перевірка повідомлення на 404
            try:
                error_data = response.json()
                if "message" in error_data and "doesn't exist" in error_data["message"]:
                    print(f"No more pages. Last attempted page: {page}")
                    break
            except ValueError:
                # Не JSON у відповіді
                print("404 received, stopping.")
                break

        elif response.status_code != 200:
            print(f"Error: received status {response.status_code}")
            break

        # Зберігаємо JSON у файл
        data = response.json()
        file_name = f"sales_{date}_{page}.json"
        file_path = os.path.join(save_path, file_name)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Saved page {page} to {file_path}")
        page += 1

