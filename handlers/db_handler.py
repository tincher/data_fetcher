import mysql.connector


class DBHandler:
    cursor = []
    db = []
    conf = {}

    def __init__(self, config):
        self.conf = config
        self.set_db_cursor()
        self.initiate_database()

    def set_db_cursor(self):
        try:
            self.db = self.connect_to_db()
            self.cursor = self.db.cursor()
        except mysql.connector.errors.ProgrammingError:
            self.db = mysql.connector.connect(
                host=self.conf['host'],
                user=self.conf['user'],
                passwd=self.conf['password'])

            self.cursor = self.db.cursor()
            self.cursor.execute('CREATE DATABASE mbot')
            self.db = self.connect_to_db()
            self.cursor = self.db.cursor()

    def connect_to_db(self):
        return mysql.connector.connect(
            host=self.conf['host'],
            user=self.conf['user'],
            passwd=self.conf['password'],
            database='mbot')

    def initiate_database(self):
        command_list = [
            'CREATE TABLE catalogue (id INT PRIMARY KEY, name VARCHAR(100), parentCategory INT)',
            'CREATE TABLE bb_items (id INT PRIMARY KEY, sku VARCHAR(30), wholesalePrice FLOAT, height FLOAT, width FLOAT, depth FLOAT)',
            'CREATE TABLE bb_item_information (id INT, name VARCHAR(100), sku VARCHAR(30), isoCode VARCHAR(2))',
            'CREATE TABLE bb_item_categories (id INT, product INT, category INT)'
        ]
        name_list = [
            'catalogue',
            'bb_items',
            'bb_item_information',
            'bb_item_categories'
        ]
        for command, name in zip(command_list, name_list):
            self.create_table(command, name)

    def table_exists(self, name):
        self.cursor.execute('SHOW TABLES')
        if len([item for item in self.cursor if item[0] == name]):
            return True
        return False

    def create_table(self, command, name):
        if not self.table_exists(name):
            self.cursor.execute(command)
            return True
        return False

    def write_to_table(self, query, values, name):
        self.cursor.execute('TRUNCATE TABLE ' + name)
        self.cursor.executemany(query, values)
        self.db.commit()

    def add_to_table(self, query, value):
        self.cursor.execute(query, value)
        self.db.commit()

    def get_all_from_table(self, name, selector):
        self.cursor.execute('SELECT ' + selector + ' FROM ' + name)
        return self.cursor.fetchall()