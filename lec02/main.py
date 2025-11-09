"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
from datetime import datetime
import os
from flask import Flask, request
from flask import typing as flask_typing

from bll.sales_api import save_sales_to_local_disk


AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")


if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")

app = Flask(__name__)

@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "date": "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')

    if not date:
        return {
            "message": "date parameter missed",
        }, 400
    
    

    try:
        # Try to parse using expected format
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {
            "message": "Invalid date format. Expected 'YYYY-MM-DD'."
            }, 400
    
    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400

    save_sales_to_local_disk(date=date, raw_dir=raw_dir, token=AUTH_TOKEN)

    return {
               "message": "Data retrieved successfully from API",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
