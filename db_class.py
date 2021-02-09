import re
from configparser import RawConfigParser
from cryptography.fernet import Fernet
import pyodbc
import time


class DbOperations:
    def __init__(self, connection_name="dev_server", autocommit=False, batch_size=500):
        # validation lists
        self.__valid_connections = ["prod_server", "dev_server"]
        # assign internal variables
        self.__key_file = "D:/Python/Projects/api_extract/config_files/fernet_key.txt"
        self.__config_file = "D:/Python/Projects/api_extract/config_files/db_connections.config"
        self.__config = RawConfigParser()
        self.__config.read(self.__config_file)
        self.__connection_name = connection_name
        self.__autocommit = autocommit
        self.__cursor = None
        self.__table_name = None
        self.__db = None
        self.__batch_size = batch_size
        self.__day_stamp = time.strftime("%Y-%m-%d")

    def __decrypt_password(self):
        # take encryption key and decrypt db pw
        with open(self.__key_file, "r", encoding="utf-8") as file:
            key = file.read()
            file.close()
        suite = Fernet(key)
        enc_pwd = self.__config.get(self.__connection_name, "pwd")
        pwd_bytes = suite.decrypt(enc_pwd.encode())
        pwd = bytes(pwd_bytes.decode("utf-8"))
        return pwd

    def __create_cursor(self):
        print(self.__connection_name)
        # get connection variables
        server = self.__config.get(self.__connection_name, "server")
        uid = self.__config.get(self.__connection_name, "uid")
        self.__db = self.__config.get(self.__connection_name, "db")
        # create connection string
        if uid.lower() == "trusted":
            connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"SERVER={server};DATABASE={self.__db};TrustedConnection=yes"
        else:
            pwd = self.__decrypt_password()
            connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + f"SERVER={server};DATABASE={self.__db};UID={uid};PWD={pwd};TrustedConnection=no"
        print(connection_string)
        conn = pyodbc.connect(connection_string, autocommit=self.__autocommit)
        self.__cursor = conn.cursor()

    def __table_exists(self):
        # look if there is a table with the given name in the db schema
        self.__cursor.execute(f"""
            select count(*)
            from information_schema.tables
            where table_name = {self.__table_name}
            """)
        if self.__cursor.fetchone()[0] == 1:
            return True
        else:
            return False

    def __create_table(self, headers):
        exists = self.__table_exists()
        # if the table exists display a message, otherwise create the table
        if not exists:
            query = f"create table {self.__db}.dbo.{self.__table_name} ({headers} varchar(max), Import varchar(max)"
            self.__cursor.execute(query)
            print(f"{self.__table_name} created")
        else:
            print(f"Table {self.__table_name} already exists")

    def __create_string(self, item_list):
        # regex variables for proper string format for SQL
        two_single_quotes_reg = re.compile(r"(?<=[^\s])''(?=\s|\)|,)|(?<=[^\s])''(?=[^\s|^\)|^,])")  # regex for finding ''
        single_quote_reg = re.compile(r"(?<=[^,(\s])(')(?=[^,^'^)\s])|(?<=\w)'(?=\w|\s)|(?<=\w\s)'(?=\w|\s)|(?<=[^,\s])'(?=\w)")  # regex for finding single quote in a name
        # extract headers - one for table creation and one for data insert
        item = item_list[0]
        keys = []
        for key in item:
            keys.append(key)
            create_headers = f'{"varchar(max), ".join(keys)}'
            insert_headers = f'{", ".join(keys)}'

        self.__create_table(headers=create_headers)

        # create variables for insert
        batch = []
        data = []
        i = 0
        for line in item_list:
            i += 1
            for value in line:
                data.append(f"'{str(line[value])}")
            data_string = ", ".join(data)
            # run regex replacements for the data string for sql import
            # remove any possible tripled single quotes that raise errors during sql upload
            # find all doubled single quotes that are in the string and replace them with a single quote
            data_string = two_single_quotes_reg.sub("'", data_string)
            # replace all single quotes not enclosing string values with doubled single quotes
            data_string = single_quote_reg.sub("''", data_string)
            data_string = data_string.replace("None", "NULL")
            # reset the data list
            data = []
            # add the sata string to the batch
            batch.append(data_string)
            # when the batch is full, upload it
            if batch == self.__batch_size:
                batch_string = "), (".join(batch)
                batch_data_string = f"({batch_string})"
                self.__sql_insert_into(headers=insert_headers, batch_data_string=batch_data_string)
                batch = []
                i = 0
        # load any leftover items
        if len(batch) > 0:
            batch_string = "), (".join(batch)
            batch_data_string = f"({batch_string})"
            self.__sql_insert_into(headers=insert_headers, batch_data_string=batch_data_string)

    def insert_data(self, table_name, insert_list):
        # check if there are any items in the insert list
        self.__table_name = table_name
        if len(insert_list) > 0:
            self.__create_string(item_list=insert_list)
        else:
            print("No items to upload")

    def __sql_insert_into(self, headers, batch_data_string):
        insert_query = f"insert into {self.__db}.dbo.{self.__table_name} {headers} values {batch_data_string}"
        insert_query = insert_query.replace("'NULL", "NULL")
        import_time_query = f"update {self.__db}.dbo.{self.__table_name} set Import = {self.__day_stamp} where Import is null"
        print(f"Insert Query:    {insert_query}")
        print(f"Import Time Query:    {import_time_query}")
        self.__cursor.execute(insert_query)
        self.__cursor.execute(import_time_query)
        self.__cursor.commit()
