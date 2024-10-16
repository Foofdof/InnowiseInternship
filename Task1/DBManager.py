import psycopg2
from psycopg2 import sql as psql


class DBManager:

    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
        self.ext = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_params['dbname'],
                user=self.db_params['user'],
                password=self.db_params['password'],
                host=self.db_params['host'],
                port=self.db_params['port'],
                client_encoding='UTF8',
            )
        except psycopg2.DatabaseError as e:
            print(f'Connection failed {e}')

    def execute(self, sql: str|psql.SQL|psql.Composed, *params, fetch : bool = False):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch:
                    return cur.fetchall()

    def executemany(self, sql: str|psql.SQL|psql.Composed, *params, fetch : bool = False):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.executemany(sql, params)
                if fetch:
                    return cur.fetchall()

    # def fetchone(self):
    #     return self.ext.fetchone()
    #
    # def fetchall(self):
    #     return self.ext.fetchall()

    def create_table(self, table_name: str, columns: dict, constraints: str = None):
        """
        :param table_name: str
        :param columns: dict {str: str}
        :param constraints: str
        :return:
        """
        columns_sql = []
        for column_name, column_type in columns.items():
            columns_sql.append(
                psql.SQL('{} {}').format(
                    psql.Identifier(column_name),
                    psql.SQL(column_type)
                )
            )

        query_str = "CREATE TABLE IF NOT EXISTS {table_name} ({columns}"
        if constraints:
            query_str += f",\n CONSTRAINT {constraints}"

        query_str += ");"
        query = psql.SQL(
            query_str
        ).format(
            table_name=psql.Identifier(table_name),
            columns=psql.SQL(', \n').join(columns_sql)
        )
        print(query.as_string(self.conn))
        self.execute(query)

    def insert_into_table(self, table_name: str, columns: tuple, values: tuple):
        query = psql.SQL(
            "INSERT INTO {table_name} ({columns}) VALUES ({values})"
        ).format(
            table_name=psql.Identifier(table_name),
            columns=psql.SQL(', ').join(map(psql.Identifier, columns)),
            values=psql.SQL(', ').join(psql.Placeholder() * len(values)),
        )
        self.execute(query, *values)

    def create_index(self, table_name: str, column: str):
        query = psql.SQL(
            """
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column});
            """
        ).format(
            table_name=psql.Identifier(table_name),
            column=psql.Identifier(column),
            index_name=psql.Identifier(f'idx_{table_name}_{column}')
        )

        self.execute(query)

    def close(self):
        if self.conn:
            self.conn.close()


def get_db(db_params: dict) -> DBManager :
    return DBManager(db_params)