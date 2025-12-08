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