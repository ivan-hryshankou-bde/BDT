import json
import psycopg2
import argparse
from dotenv import dotenv_values
from abc import ABC, abstractmethod
import xml.etree.ElementTree as ET
from psycopg2.extras import RealDictCursor

config = dotenv_values(".env")

# Database configuration
DB_CONFIG = {
    'dbname': config["DB_NAME"],
    'user': config["DB_USER"],
    'password': config["DB_PASSWORD"],
    'host': config["DB_HOST"],
    'port': config["DB_PORT"]
}

class DatabaseConnector:
    """Context manager class for a database connection"""
    def __init__(self, config):
        self.config = config
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(**self.config)
        return self.conn

    def __exit__(self, type, value, traceback):
        if self.conn:
            self.conn.close()

class IOutputFormatter(ABC):
    """Astract classs for formatters"""
    @abstractmethod
    def format(self, data):
        pass

class JsonFormatter(IOutputFormatter):
    """Realization of JSON output formatter"""
    def format(self, data):
        return json.dumps(data, indent=4, default=str)

class XmlFormatter(IOutputFormatter):
    """Realization of XML output formatter"""
    def format(self, data):
        root = ET.Element("report")
        for query_name, records in data.items():
            query_elem = ET.SubElement(root, query_name)
            for record in records:
                item = ET.SubElement(query_elem, "item")
                for key, value in record.items():
                    child = ET.SubElement(item, key)
                    child.text = str(value)
        return ET.tostring(root, encoding='unicode')

class SchemaManager:
    """Schema managment class for DDL operations"""
    def __init__(self, conn):
        self.conn = conn

    def create_schema(self):
        """Creates schema: dormitory in database if not exists"""
        with self.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS dormitory;")

        self.conn.commit()

    def create_tables(self):
        """Recreates tables inside dormitory schema"""
        with self.conn.cursor() as cur:
            # 1. Deleting tables if exists, where students go first
            cur.execute("DROP TABLE IF EXISTS dormitory.students")
            cur.execute("DROP TABLE IF EXISTS dormitory.rooms")
            
            # 2. Creating rooms table
            cur.execute("""
                CREATE TABLE dormitory.rooms (
                    id INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                )
            """)
            
            # 3. Creating students table
            cur.execute("""
                CREATE TABLE dormitory.students (
                    id INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    birthday TIMESTAMP NOT NULL,
                    room_id INT,
                    sex CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
                    FOREIGN KEY (room_id) REFERENCES dormitory.rooms(id)
                )
            """)

        self.conn.commit()

    def create_indexes(self):
        """Creates indexes for optimization"""
        # Indexes based on usage patterns:
        # 1. room_id: Because of JOINs and GROUP BY on room_id
        # 2. birthday: Because of calculating Age and Ordering
        # 3. sex: Because of the mixed-sex query, combined with room
        
        # Creating indexes just once if not exists
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_students_room ON dormitory.students(room_id)",
            "CREATE INDEX IF NOT EXISTS idx_students_birthday ON dormitory.students(birthday)",
            "CREATE INDEX IF NOT EXISTS idx_students_room_sex ON dormitory.students(room_id, sex)"
        ]
        
        with self.conn.cursor() as cur:
            for query in index_queries:
                cur.execute(query)
        
        self.conn.commit()

class DataLoader:
    """Class for extracting and loading data a.k.a ETL processing"""
    def __init__(self, conn):
        self.conn = conn

    def load_data(self, rooms_path, students_path):
        with open(rooms_path, 'r') as f:
            rooms_data = json.load(f)
            rooms_values = ((r['id'], r['name']) for r in rooms_data)

        with open(students_path, 'r') as f:
            students_data = json.load(f)
            students_values = (
                (s['id'], s['name'], s['birthday'], s['room'], s['sex']) 
                for s in students_data
            )

        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO dormitory.rooms (id, name) VALUES (%s, %s)", 
                rooms_values)
            
            cur.executemany(
                "INSERT INTO dormitory.students (id, name, birthday, room_id, sex) VALUES (%s, %s, %s, %s, %s)", 
                students_values)
        
        self.conn.commit()

class ReportGenerator:
    """Class for realizing DQL operations"""
    def __init__(self, conn):
        self.conn = conn

    def get_rooms_with_counts(self):
        """List of rooms and the number of students in each of them"""
        sql = """
            SELECT r.name, COUNT(s.id) as student_count
            FROM dormitory.rooms r
            LEFT JOIN dormitory.students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            ORDER BY r.id
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_top_5_smallest_avg_age(self):
        """5 rooms with the smallest average age of students"""
        sql = """
            SELECT r.name, AVG(EXTRACT(YEAR FROM AGE(NOW(), s.birthday))) as avg_age
            FROM dormitory.rooms r
            JOIN dormitory.students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            ORDER BY avg_age ASC
            LIMIT 5
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_top_5_largest_age_diff(self):
        """5 rooms with the largest difference in the age of students"""
        sql = """
            SELECT r.name, 
                   (MAX(EXTRACT(YEAR FROM AGE(NOW(), s.birthday))) - 
                    MIN(EXTRACT(YEAR FROM AGE(NOW(), s.birthday)))) as age_diff
            FROM dormitory.rooms r
            JOIN dormitory.students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            ORDER BY age_diff DESC
            LIMIT 5
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_mixed_sex_rooms(self):
        """List of rooms where different-sex students live"""
        sql = """
            SELECT r.name
            FROM dormitory.rooms r
            JOIN dormitory.students s ON r.id = s.room_id
            GROUP BY r.id, r.name
            HAVING COUNT(DISTINCT s.sex) > 1
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

def main():
    """Main controller of the module brining all classes together"""
    parser = argparse.ArgumentParser(description='Dormitory ETL and Report')
    parser.add_argument('students', help='Path to students file')
    parser.add_argument('rooms', help='Path to rooms file')
    parser.add_argument('format', choices=['json', 'xml'], help='Output format')
    
    args = parser.parse_args()

    # Logic for formatter
    formatter = JsonFormatter() if args.format == 'json' else XmlFormatter()

    try:
        with DatabaseConnector(DB_CONFIG) as conn:
            # 1. Init Schema
            schema = SchemaManager(conn)
            print("Creating schema...")
            schema.create_schema()
            schema.create_tables()
            print("Schema created.")
            
            # 2. Load Data
            loader = DataLoader(conn)
            print("Loading data...")
            loader.load_data(args.rooms, args.students)
            print("Data loaded.")
            
            # 3. Add Indexes
            print("Creating indexes...")
            schema.create_indexes()
            print("Indexes created.")

            # 4. Generate Reports
            reporter = ReportGenerator(conn)
            print("Generating report...")
            results = {
                "rooms_with_counts": reporter.get_rooms_with_counts(),
                "smallest_avg_age": reporter.get_top_5_smallest_avg_age(),
                "largest_age_diff": reporter.get_top_5_largest_age_diff(),
                "mixed_sex_rooms": reporter.get_mixed_sex_rooms()
            }
            print("Report generated.")

            # 5. Output
            output = formatter.format(results)
            print("\n--- REPORT OUTPUT ---")
            print(output)

    except psycopg2.Error as db_err:
        print(f"Database Error: {db_err}")
    except (IOError, json.JSONDecodeError) as file_err:
        print(f"File error: {file_err}")
    except Exception as e:
        print(f"Another error: {e}")

if __name__ == "__main__":
    main()