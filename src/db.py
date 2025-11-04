import sqlite3


class PodDatabase:
    """
    Creates a database to hold data about pods.
    """

    def __init__(self, path: str):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS pods (
                uid TEXT PRIMARY KEY,
                namespace TEXT,
                name TEXT,
                node TEXT,
                cpu TEXT,
                memory REAL,
                gpu REAL,
                start_time TEXT,
                end_time TEXT,
                end_time_guessed INTEGER DEFAULT 0
            )
        """)

        self.connection.commit()

    def insert_new_pod(self, uid, namespace, name, node, cpu, memory, gpu, start_time):
        """Insert a new pod record."""
        self.cursor.execute(
            """
            INSERT OR IGNORE INTO pods (uid, namespace, name, node, cpu, memory, gpu, start_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (uid, namespace, name, node, cpu, memory, gpu, start_time),
        )
        self.connection.commit()

    def update_pod_end_time(self, uid, end_time, guessed=False):
        """Update end_time and guessed flag for an existing pod."""
        self.cursor.execute(
            """
            UPDATE pods
            SET end_time = ?, end_time_guessed = ?
            WHERE uid = ?
        """,
            (end_time, int(guessed), uid),
        )
        self.connection.commit()

    def get_finished_pods(self):
        """Returns a set of uid of all pods that finished according to the database"""
        results = self.cursor.execute(
            """SELECT uid FROM pods WHERE end_time is NOT NULL"""
        )
        return {result[0] for result in results}

    def get_running_pods(self):
        """Returns a set of uid of all running pods according to the database"""
        results = self.cursor.execute(
            """SELECT uid FROM pods WHERE end_time is NULL"""
        )
        return {result[0] for result in results}

    def close(self):
        self.connection.close()
