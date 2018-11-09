import os

def connect(file_path):
    return Connection(file_path)


class Cursor:
    def __init__(self, file_path):
        self.file_path = file_path

        self.closed = False
        self.description = ""
        self.row_count = 0

        self.results = []

    def close(self):
        self.closed = True

    def execute(self, query):
        # Currently must pass in direct sql string ie 'SELECT * from whatever_table;' ie sqlparamstyle='direct' 
        # Check if the file exists
        # If so run the command (sqlite3 + filepath + query/command)
        results = os.popen('sqlite3 {} "{}"'.format(self.file_path, query)).read().split()
        self.results = results
        return results

    def fetch_one(self):
        if self.results:
            return self.results.pop(0)

        return []  # Or possible raise error

    def fetch_all(self):
        pass
    # def executeMany(self, query)


# decorator
# def isClosed(f):
    # I shouldn't return a function AND a string

    # if self.closed == True:
    #     return "ERROR"

    # return f


class Connection:
    def __init__(self, file_path):
        self.file_path = file_path
        self.closed = False

    def close(self):
        self.closed = True

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self.file_path)

    def create_table(self, table_name, columns):
        cursor = self.cursor()

        create_table_sql = 'CREATE TABLE {} (id INTEGER PRIMARY KEY'.format(table_name)
        for column in columns:
            create_table_sql += ', {} TEXT NOT NULL'.format(column)
        create_table_sql += ');'

        cursor.execute(create_table_sql)
        cursor.close()

        # columns = [...]
        # At first I can default to strings for everything except the primary key
        # CREATE TABLE people (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL, email text NOT NULL);
        # Must check that table exists

    def list_tables(self):
        cursor = self.cursor()
        tables = cursor.execute('.tables')
        cursor.close()
        return tables

# TESTS
# can connect, and can create a connection that has a #cursor, close, execute, create_table, list_table method
con = connect('testdb.db')

#Listing tables lists the expected tables (maybe make some tables to start with)
# Creating a table actually creates a table
print(con.list_tables())
con.create_table('people5', ['a', 'b', 'c_col'])
print(con.list_tables())

# Cursor construction with close, execute, fetchone methods
# Can select values, insert values, maybe test a few different sql commands
#  select *
#  insert
#  select * where...
#  join blah
c = con.cursor()
print(c.execute('SELECT * FROM people3;'))
c.execute('INSERT INTO people5 (a, b, c_col) VALUES (1,2,3), (4,5,6), (7,8,9);')

import pdb; pdb.set_trace()
print(c.execute('SELECT * FROM people5;'))
# c.execute('select *;')


# Test Data
# people
# id  first_name last_name email          age
# 1     bob        saget   bob@gmail      54
# 2     bob        ross   ross@gmail      79
# 3     julia     test    julia@gmail      22   
# 4     tex       mex   abc@abc.net      100
# 5     bill      burr   hello@gmail     9
