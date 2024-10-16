import os
from dotenv import load_dotenv
from pandas import DataFrame
from .DBManager import get_db, DBManager
from .decors import time_it
import pandas as pd


load_dotenv(encoding='utf-8')

db_params = {
    'dbname' : str(os.getenv('PG_DATABASE')),
    'user' : str(os.getenv('PG_USER')),
    'password' : str(os.getenv('PG_PASS')),
    'host' : str(os.getenv('PG_HOST')),
    'port' : str(os.getenv('PG_PORT')),
}


def read_data(rooms_path : str, student_path : str) -> tuple[DataFrame, DataFrame]:
    rooms = pd.read_json(rooms_path)
    rooms['id'] = rooms['id'].astype(int)
    students = pd.read_json(student_path)
    return rooms, students


def db_init(db : DBManager, rooms_columns, students_columns):
    db.connect()

    db.create_table('rooms', {
        rooms_columns[0]: 'SERIAL PRIMARY KEY',
        rooms_columns[1]: 'varchar NOT NULL',
    })

    db.create_table('students', {
        students_columns[0]: 'TIMESTAMP NOT NULL',
        students_columns[1]: 'SERIAL PRIMARY KEY',
        students_columns[2]: 'varchar NOT NULL',
        students_columns[3]: 'INTEGER NOT NULL',
        students_columns[4]: 'BOOLEAN NOT NULL'
    }, 'fk_room FOREIGN KEY(room) REFERENCES rooms(id)')

    db.create_index('students', 'room')
    db.create_index('rooms', 'name')
    db.create_index('students', 'name')

    db.close()


def load_data(db : DBManager, rooms, students):
    db.connect()

    for item in rooms.itertuples(index=True):
        try:
            db.insert_into_table('rooms', rooms.columns.to_list(), (int(item.id), item.name))
        except Exception as e:
            pass

    for item in students.itertuples(index=True):
        try:
            db.insert_into_table('students', students.columns.to_list(),
                (
                    item.birthday,
                    int(item.id),
                    item.name,
                    int(item.room),
                    True if item.sex == 'M' else False,
                )
            )
        except Exception as e:
            pass

    db.close()

@time_it(1)
def get_queryset(db : DBManager, sql: str):
    db.connect()
    result = db.execute(sql, fetch=True)
    db.close()

    return result

def run(format: str):
    rooms, students = read_data('Task1/raw_data/rooms.json','Task1/raw_data/students.json')

    print(rooms.head(3))
    print(students.head(3))
    print('\n')

    db = get_db(db_params)
    db_init(db, rooms.columns, students.columns)
    # load_data(db, rooms, students)

    sql_list = [
    """
        SELECT 
            DISTINCT rooms.name,
            COUNT(DISTINCT students.name)
        FROM rooms
        INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.name
    """,
    """
        SELECT 
            DISTINCT rooms.name,
            AVG(EXTRACT(YEAR FROM AGE(students.birthday))) as age
        FROM rooms
        INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.name
        ORDER BY age ASC
        LIMIT 5
    """,
    """
        SELECT 
            DISTINCT rooms.name,
            MAX(EXTRACT(YEAR FROM AGE(students.birthday))) - MIN(EXTRACT(YEAR FROM AGE(students.birthday))) as age_diff
        FROM rooms
        INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.name
        ORDER BY age_diff DESC
        LIMIT 5
    """,
    """
        SELECT 
            DISTINCT rooms.name
        FROM rooms
        INNER JOIN students ON rooms.id = students.room
        GROUP BY rooms.name
        HAVING 
            NOT BOOL_AND(students.sex) AND
            BOOL_OR(students.sex)
        """
    ]

    results = [get_queryset(db, sql) for sql in sql_list]


    answer1 = pd.DataFrame(results[0], columns=['room_name', 'num_students'])
    print('\n', 'Answer 1:', sep='\n')
    print(answer1.head(3))

    answer2 = pd.DataFrame(results[1], columns=['room_name', 'age_avg'])
    print('\n', 'Answer 2:', sep='\n')
    print(answer2)

    answer3 = pd.DataFrame(results[2], columns=['room_name', 'age_diff'])
    print('\n', 'Answer 3:', sep='\n')
    print(answer3)

    answer4 = pd.DataFrame(results[3], columns=['room_name'])
    print('\n', 'Answer 4:', sep='\n')
    print(answer4.head(5))

    answers = [answer1, answer2, answer3, answer4]

    os.makedirs(f'Task1/processed_data/{format}', exist_ok=True)
    for i, answer in enumerate(answers, start=1):
        if format == 'csv':
            answer.to_csv(f'Task1/processed_data/{format}/task1_{i}.csv',
                          index=False,
                          encoding='utf-8',)
        if format == 'json':
            answer.to_json(f'Task1/processed_data/{format}/task1_{i}.json',
                           orient='records',
                           force_ascii=False,
                           indent=4, )
        else:
            answer.to_xml(f'Task1/processed_data/{format}/task1_{i}.xml',
                          index=False,
                          root_name='rooms',
                          row_name='room',
                          encoding='utf-8',)
