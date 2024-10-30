import os
import pandas as pd
import json
from elasticsearch import Elasticsearch, helpers
import logging
import sys
import csv
import fileinput
import re

# Initialize Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

def setup_logger():
    logger = logging.getLogger('bulk_indexing')
    logger.setLevel(logging.ERROR)
    handler = logging.FileHandler('error.log')
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def read_mysql(file_path):
    def is_insert(line):
        return line.startswith('INSERT INTO') or line.startswith('insert into')

    def get_values(line):
        return line.partition(' VALUES ')[2]

    def values_sanity_check(values):
        assert values
        assert values[0] == '('
        return True

    def parse_values(values):
        latest_row = []
        reader = csv.reader([values], delimiter=',', doublequote=False, escapechar='\\', quotechar="'", strict=True)
        rows = []
        for reader_row in reader:
            for column in reader_row:
                if len(column) == 0 or column == 'NULL':
                    latest_row.append(chr(0))
                    continue
                if column[0] == "(":
                    if len(latest_row) > 0 and latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        rows.append(latest_row)
                        latest_row = []
                    if len(latest_row) == 0:
                        column = column[1:]
                latest_row.append(column)
            if latest_row[-1][-2:] == ");":
                latest_row[-1] = latest_row[-1][:-2]
                rows.append(latest_row)
        return rows

    data = []
    try:
        with fileinput.input(files=(file_path,)) as f:
            for line in f:
                if not is_insert(line):
                    continue
                values = get_values(line)
                if not values_sanity_check(values):
                    raise Exception("Getting substring of SQL INSERT statement after ' VALUES ' failed!")
                data.extend(parse_values(values))
    except KeyboardInterrupt:
        sys.exit(0)   
    result = [{f"data{i+1}": value for i, value in enumerate(item)} for item in data]
    return result

def log_bulk_indexing_errors(errors, logger):
    for error in errors:
        error_details = json.dumps(error, indent=2)
        logger.error(f"Error: {error_details}")

def create_index_name(file_path):
    index_name = file_path.replace('/', '-').replace('\\', '-')
    return index_name.lower()

def read_csv(file_path):
    return pd.read_csv(file_path, dtype=str).to_dict(orient='records')

def read_tsv(file_path):
    return pd.read_csv(file_path, dtype=str,sep='\t').to_dict(orient='records')

def read_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data if isinstance(data, list) else [data]

def read_plaintext(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return [{'line': line.strip()} for line in lines]

def read_postgresql(file_path):
    result = []
    in_copy_section = False
    columns = []

    with open(file_path, 'r') as file:
        for line in file:
            try:
                if "COPY" in line:
                    # Extract the columns from the COPY line
                    columns = line.split("(")[1].split(")")[0].split(", ")
                    print(len(columns))
                    in_copy_section = True
                elif in_copy_section:
                    if line.strip() == "\\.":
                        break
                    if "\t" in line:
                        # Split the line by tabs for each value
                        values = line.strip().split('\t')
                        # Create a dictionary using the columns and corresponding values
                        try:
                            result.append({columns[i]: values[i] if values[i] != '\\N' else None for i in range(len(values))})
                        except:
                            print(f"Error in this lines : \n{line}" )
                            pass
            except:
                pass
    return result

def check_string_in_file(file_path):
    with open(file_path, 'r') as file:
        i = 0
        for line in file:
            if "PostgreSQL" in line:
                return "PostgreSQL"
            if "MySQL" in line:
                return "MySQL"
            if i == 10:
                return False
            i = i+1
    return False


def ingest_to_elasticsearch(file_path):
    _, file_extension = os.path.splitext(file_path)
    index_name = create_index_name(file_path)
    
    if file_extension == '.csv':
        data = read_csv(file_path)
    elif file_extension == '.tsv':
        data = read_tsv(file_path)
    elif file_extension == '.json':
        data = read_json(file_path)
    elif file_extension == '.txt':
        data = read_plaintext(file_path)
    elif file_extension == '.sql':
        if check_string_in_file(file_path) == "PostgreSQL":
            data = read_postgresql(file_path)
        elif check_string_in_file(file_path) == "MySQL":
            data = read_mysql(file_path)
        else:
            raise TypeError(f"Found unsupported SQL dump file : {file_path}")
    else:
        raise TypeError(f"Skipping unsupported file: {file_path}")
    # index_settings = {
    # "settings": {
    #     "index": {
    #         "description": f"{file_path}"
    #     }
    # }
    # }   

    # es.indices.create(index=f"i{index_name}", body=index_settings)

    print(type(data[0]))
    print(data[0])
    # for i in range(10):
    #     print(data[i])

    actions = [
        {
            "_index": f"i{index_name}",
            "_source": record
        }
        for record in data
    ]
    
    success, failed = 0, 0
    errors = []
    logger = setup_logger()
    # Use helpers.bulk with detailed error logging
    for ok, response in helpers.streaming_bulk(es, actions):
        if not ok:
            failed += 1
            errors.append(response)
        else:
            success += 1
    
    print(f"Successfully indexed {success} documents.")
    if failed:
        log_bulk_indexing_errors(errors, logger)
    return success


def crawl_and_ingest(root_directory):
    supported_extensions = {'.csv', '.tsv', '.json', '.txt','.sql'}
    for root, dirs, files in os.walk(root_directory):
        for file in files:
            full_paths = ""
            full_path = os.path.join(root, file)
            full_paths = full_paths + full_path
            _, file_extension = os.path.splitext(full_paths)
            if file_extension in supported_extensions:
                try:
                    # print(file_path)
                    ingest_to_elasticsearch(full_paths)
                except Exception as e:
                    print(f"[ERROR] Error processing file {full_paths}: {e}")
            else:
                print(f"[WARNING] Unsupported file extension: {full_paths}")

def ingestAsTxt(file_path):
    _, file_extension = os.path.splitext(file_path)
    index_name = create_index_name(file_path)
    data = read_plaintext(file_path)
    actions = [
        {
            "_index": f"i{index_name}",
            "_source": record
        }
        for record in data
    ]
    success, failed = 0, 0
    errors = []
    logger = setup_logger()
    # Use helpers.bulk with detailed error logging
    for ok, response in helpers.streaming_bulk(es, actions):
        if not ok:
            failed += 1
            errors.append(response)
        else:
            success += 1
    print(f"Successfully indexed {success} documents.")
    if failed:
        log_bulk_indexing_errors(errors, logger)


# Example usage Direct Call
root_directory = ''
crawl_and_ingest(root_directory)