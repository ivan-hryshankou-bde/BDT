from psycopg2.extras import RealDictCursor

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