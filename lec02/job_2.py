"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
from datetime import datetime
import os
from flask import Flask, request
from flask import typing as flask_typing

from bll.sales_api import save_sales_to_local_disk
from dal.process_files import process_files


app = Flask(__name__)

@app.route('/', methods=['POST'])
def run_job_2() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    stg_dir = input_data.get('stg_dir')
    raw_dir = input_data.get('raw_dir')

    if not stg_dir:
        return {
            "message": "stg_dir parameter is missing",
        }, 400
    
    if not raw_dir:
        return {
            "message": "raw_dir parameter is missing",
        }, 400
    

    process_files(raw_dir=raw_dir, stg_dir=stg_dir)

    return {
               "message": "Data request processed successfully",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
