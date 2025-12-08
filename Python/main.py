import json
import psycopg2
import argparse
import logging
from dotenv import dotenv_values

# Import of classes from within the modules
from src import (
    DatabaseConnector, 
    SchemaManager, 
    DataLoader, 
    ReportGenerator, 
    JsonFormatter, 
    XmlFormatter
)

# Configure of logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("report.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def main():
    """Main controller of the module brining all classes together"""
    parser = argparse.ArgumentParser(description='Dormitory ETL and Report')
    parser.add_argument('students', help='Path to students file')
    parser.add_argument('rooms', help='Path to rooms file')
    parser.add_argument('format', choices=['json', 'xml'], help='Output format')
    
    args = parser.parse_args()

    # Database configuration loading
    config = dotenv_values(".env")

    DB_CONFIG = {
        'dbname': config["DB_NAME"],
        'user': config["DB_USER"],
        'password': config["DB_PASSWORD"],
        'host': config["DB_HOST"],
        'port': config["DB_PORT"]
    }

    # Logic for formatter
    formatter = JsonFormatter() if args.format == 'json' else XmlFormatter()

    try:
        with DatabaseConnector(DB_CONFIG) as conn:
            # 1. Init Schema
            schema = SchemaManager(conn)
            logging.info("Creating schema...")
            schema.create_schema()
            schema.create_tables()
            logging.info("Schema created.")
            
            # 2. Load Data
            loader = DataLoader(conn)
            logging.info("Loading data...")
            loader.load_data(args.rooms, args.students)
            logging.info("Data loaded.")
            
            # 3. Add Indexes
            logging.info("Creating indexes...")
            schema.create_indexes()
            logging.info("Indexes created.")

            # 4. Generate Reports
            reporter = ReportGenerator(conn)
            logging.info("Generating report...")
            results = {
                "rooms_with_counts": reporter.get_rooms_with_counts(),
                "smallest_avg_age": reporter.get_top_5_smallest_avg_age(),
                "largest_age_diff": reporter.get_top_5_largest_age_diff(),
                "mixed_sex_rooms": reporter.get_mixed_sex_rooms()
            }
            logging.info("Report generated.")

            # 5. Output
            output_data = formatter.format(results)
            output_filename = f"report.{args.format}"
            
            logging.info(f"Saving report to file: {output_filename}...")
            with open(output_filename, "w", encoding='utf-8') as f:
                f.write(output_data)
            
            logging.info("Process finished successfully.")

    except psycopg2.Error as db_err:
        logging.error(f"Database Error: {db_err}")
    except (IOError, json.JSONDecodeError) as file_err:
        logging.error(f"File error: {file_err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()