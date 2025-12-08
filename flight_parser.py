#!/usr/bin/env python3
"""
Python Final Assignment: Flight Schedule Parser and Query Tool

Usage examples:
    python flight_parser.py -i data/db.csv
    python flight_parser.py -d data/flights/
    python flight_parser.py -j db.json
    python flight_parser.py -j db.json -q data/query.json
"""

import argparse      #for reading command-line arguments
import csv           #\
import json           #for file handling (CSV files, JSON files, paths).
import os            #/
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

STUDENT_ID = "231ADB279"      
STUDENT_NAME = "Aysel"        
STUDENT_LASTNAME = "Abiyeva"  

DEFAULT_DB_JSON = "db.json"
ERRORS_TXT = "errors.txt"

DATETIME_FORMAT = "%Y-%m-%d %H:%M"

#Validation Functions

def is_valid_flight_id(value: str) -> bool:
    return value.isalnum() and 2 <= len(value) <= 8


def is_valid_airport_code(value: str) -> bool:
    # 3 uppercase letters
    return len(value) == 3 and value.isalpha() and value.isupper()


def parse_datetime(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, DATETIME_FORMAT)
    except ValueError:
        return None


def parse_price(value: str) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        return None



#  CORE: CSV PARSING / VALIDATE


def validate_row(fields: List[str], line_no: int, original_line: str) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """
    Validate a single CSV row.
    Returns:
      (is_valid, flight_dict_or_None, error_message_if_invalid)
    """
    reasons: List[str] = []

    if len(fields) != 6:
        reasons.append("missing required fields")
        return False, None, f"Line {line_no}: {original_line} \u2192 {', '.join(reasons)}"

    flight_id, origin, destination, dep_str, arr_str, price_str = (f.strip() for f in fields)

    # Flight ID
    if not flight_id:
        reasons.append("missing flight_id field")
    elif not is_valid_flight_id(flight_id):
        if len(flight_id) > 8:
            reasons.append("flight_id too long (more than 8 characters)")
        else:
            reasons.append("invalid flight_id format")

    # Origin
    if not origin:
        reasons.append("missing origin field")
    elif not is_valid_airport_code(origin):
        reasons.append("invalid origin code")

    # Destination
    if not destination:
        reasons.append("missing destination field")
    elif not is_valid_airport_code(destination):
        reasons.append("invalid destination code")

    # Datetimes
    dep_dt = parse_datetime(dep_str)
    arr_dt = parse_datetime(arr_str)

    if dep_dt is None and arr_dt is None:
        reasons.append("invalid date format")
    else:
        if dep_dt is None:
            reasons.append("invalid departure datetime")
        if arr_dt is None:
            reasons.append("invalid arrival datetime")

    # Arrival after departure
    if dep_dt is not None and arr_dt is not None:
        if arr_dt <= dep_dt:
            reasons.append("arrival before departure")

    # Price
    price = parse_price(price_str)
    if price is None:
        reasons.append("invalid price value")
    else:
        if price < 0:
            reasons.append("negative price value")
        elif price == 0:
            reasons.append("price must be positive")

    if reasons:
        return False, None, f"Line {line_no}: {original_line} \u2192 {', '.join(reasons)}"

    # Construct valid flight dict
    flight = {
        "flight_id": flight_id,
        "origin": origin,
        "destination": destination,
        "departure_datetime": dep_str,
        "arrival_datetime": arr_str,
        "price": price,
    }
    return True, flight, ""

#Parse a single CSV file, write errors to ERRORS_TXT, and return list of valid flights.

def parse_csv_file(path: str) -> List[Dict[str, Any]]:

    valid_flights: List[Dict[str, Any]] = []

    with open(path, "r", encoding="utf-8") as f, open(ERRORS_TXT, "a", encoding="utf-8") as err_f:
        reader = f.readlines()

        header_skipped = False
        for line_no, raw_line in enumerate(reader, start=1):
            original_line = raw_line.rstrip("\n")

            stripped = original_line.strip()
            if not stripped:
                # blank line -> ignore completely
                continue

            # Header
            if not header_skipped and stripped.lower().startswith("flight_id,origin,destination"):
                header_skipped = True
                continue

            # Comment lines
            if stripped.startswith("#"):
                msg = f"Line {line_no}: {original_line} \u2192 comment line, ignored for data parsing"
                err_f.write(msg + "\n")
                continue

            # Normal data line
            # Use csv.reader on a single line to be safe
            for fields in csv.reader([original_line]):
                is_valid, flight, error_msg = validate_row(fields, line_no, original_line)
                if is_valid and flight is not None:
                    valid_flights.append(flight)
                else:
                    err_f.write(error_msg + "\n")

    return valid_flights

#Parse all .csv files in a folder and combine results.

def parse_csv_folder(folder_path: str) -> List[Dict[str, Any]]:
    
    all_flights: List[Dict[str, Any]] = []

    # Clear/overwrite previous errors.txt
    if os.path.exists(ERRORS_TXT):
        os.remove(ERRORS_TXT)

    csv_files = [
        os.path.join(folder_path, name)
        for name in os.listdir(folder_path)
        if name.lower().endswith(".csv")
    ]
    csv_files.sort()

    for path in csv_files:
        flights = parse_csv_file(path)
        all_flights.extend(flights)

    return all_flights

#Parse a single CSV file (helper so we can clear errors.txt first).

def parse_single_csv(path: str) -> List[Dict[str, Any]]:

    if os.path.exists(ERRORS_TXT):
        os.remove(ERRORS_TXT)
    return parse_csv_file(path)

#  JSON DB LOAD / SAVE

def save_db_json(flights: List[Dict[str, Any]], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(flights, f, indent=2, ensure_ascii=False)


def load_db_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON database must be a list of flight objects")
    return data


#  QUERY HANDLING

def load_queries(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("Query JSON must be an object or an array of objects")

#Apply filtering rules

def flight_matches_query(flight: Dict[str, Any], query: Dict[str, Any]) -> bool:
    
    # Exact matches
    for key in ("flight_id", "origin", "destination"):
        if key in query:
            if str(flight.get(key)) != str(query[key]):
                return False

    # Date/time filters
    if "departure_datetime" in query:
        q_dep = parse_datetime(query["departure_datetime"])
        if q_dep is None:
            return False
        f_dep = parse_datetime(flight["departure_datetime"])
        if f_dep is None or f_dep < q_dep:
            return False

    if "arrival_datetime" in query:
        q_arr = parse_datetime(query["arrival_datetime"])
        if q_arr is None:
            return False
        f_arr = parse_datetime(flight["arrival_datetime"])
        if f_arr is None or f_arr > q_arr:
            return False

    # Price filter
    if "price" in query:
        try:
            q_price = float(query["price"])
        except (TypeError, ValueError):
            return False
        if float(flight["price"]) > q_price:
            return False

    return True


def run_queries_on_db(flights: List[Dict[str, Any]], queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    For each query, produce:
    {
        "query": { ... },
        "matches": [ flight1, flight2, ... ]
    }
    """
    responses = []
    for q in queries:
        matches = [flight for flight in flights if flight_matches_query(flight, q)]
        responses.append({
            "query": q,
            "matches": matches
        })
    return responses

#Save responses to response_<studentid>_<name>_<lastname>_<YYYYMMDD_HHMM>.json. Returns the filename

def save_query_response(responses: List[Dict[str, Any]]) -> str:
    
    now = datetime.now()
    filename = f"response_{STUDENT_ID}_{STUDENT_NAME}_{STUDENT_LASTNAME}_{now.strftime('%Y%m%d_%H%M')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(responses, f, indent=2, ensure_ascii=False)
    return filename

#  CLI / MAIN

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Flight Schedule Parser and Query Tool"
    )
    parser.add_argument(
        "-i", "--input-file",
        help="Parse a single CSV file."
    )
    parser.add_argument(
        "-d", "--input-dir",
        help="Parse all .csv files in a folder and combine results."
    )
    parser.add_argument(
        "-o", "--output-json",
        help=f"Optional custom output path for valid flights JSON (default: {DEFAULT_DB_JSON})."
    )
    parser.add_argument(
        "-j", "--json-db",
        help="Load existing JSON database instead of parsing CSVs."
    )
    parser.add_argument(
        "-q", "--query",
        help="Execute queries defined in a JSON file on the loaded database."
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    # Decide how to get the database: CSV parse or JSON load
    flights_db: List[Dict[str, Any]] = []

    # If JSON DB is provided, use that instead of parsing CSVs
    if args.json_db:
        if args.input_file or args.input_dir:
            print("Warning: -j provided, ignoring -i/-d CSV inputs (using existing JSON database).")
        try:
            flights_db = load_db_json(args.json_db)
        except Exception as e:
            print(f"Error loading JSON database '{args.json_db}': {e}")
            return
    else:
        # We must parse CSV(s)
        if not args.input_file and not args.input_dir:
            parser.print_help()
            print("\nError: you must specify either -i (file) or -d (folder) unless using -j.")
            return

        try:
            if args.input_file:
                flights_db = parse_single_csv(args.input_file)
            elif args.input_dir:
                flights_db = parse_csv_folder(args.input_dir)
        except FileNotFoundError as e:
            print(f"File/folder not found: {e}")
            return
        except Exception as e:
            print(f"Error while parsing CSV: {e}")
            return

        # Save parsed DB to JSON
        output_path = args.output_json if args.output_json else DEFAULT_DB_JSON
        try:
            save_db_json(flights_db, output_path)
            print(f"Saved {len(flights_db)} valid flights to '{output_path}'.")
            print(f"Errors (if any) are in '{ERRORS_TXT}'.")
        except Exception as e:
            print(f"Error saving JSON database: {e}")
            return

    # If -q is provided, run queries on the database
    if args.query:
        try:
            queries = load_queries(args.query)
        except Exception as e:
            print(f"Error loading query JSON '{args.query}': {e}")
            return

        responses = run_queries_on_db(flights_db, queries)
        try:
            response_file = save_query_response(responses)
            print(f"Query responses saved to '{response_file}'.")
        except Exception as e:
            print(f"Error saving query response file: {e}")
            return
    else:
        # No queries, just info
        print(f"Loaded {len(flights_db)} flights in database.")


if __name__ == "__main__":
    main()
