import json

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
