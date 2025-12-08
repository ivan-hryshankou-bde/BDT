import psycopg2

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