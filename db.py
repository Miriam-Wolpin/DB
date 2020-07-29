

from json_function import read_from_json, write_to_json, add_to_json
import os
from typing import List, Dict, Any
from bplustree import BPlusTree
import db_api
from db_api import DBField, SelectionCriteria
MAX_NUM_OF_LINES_IN_FILE = 1000
def end_place_in_file(data, name):
    return not data[name]['num of lines'] % MAX_NUM_OF_LINES_IN_FILE

# def update_index(indexes, path, values):
#     for index in indexes:

def delete_table_from_db(table_name):
    db_data = read_from_json("db_files/db.json")
    num_of_files = db_data[table_name]['num of files']
    del (db_data[table_name])
    db_data['num of tables'] -= 1
    write_to_json("db_files/db.json", db_data)
    return num_of_files

def delete_files_of_table(table_name, num):
    for i in range(num):
        if os.path.exists(f"db_files/{table_name}_{i + 1}"):
            os.remove(f"db_files/{table_name}_{i + 1}")

def convert_from_DBfield_to_dict(fields):
    return [{field.name: field.type.__name__} for field in fields]

def convert_from_dict_to_DBfield(fields):
    pass

class DBTable:
    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        db_data = read_from_json("db_files/db.json")
        return db_data[self.name]['num of lines']

    def insert_record(self, values: Dict[str, Any]) -> None:
        db_data = read_from_json("db_files/db.json")
        primary_key = db_data[self.name]['primary key']
        record_to_insert = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}

        if end_place_in_file(db_data, self.name) and db_data[self.name]['num of lines'] != 0:
            db_data[self.name]['num of files'] += 1
            write_to_json(f"db_files/{self.name}_{db_data[self.name]['num of files']}.json", record_to_insert)
        else:
            add_to_json(f"db_files/{self.name}_{db_data[self.name]['num of files']}.json", record_to_insert)

        db_data[self.name]['num of lines'] += 1
        write_to_json("db_files/db.json", db_data)

        # update_index(db_data[self.name]['indexes'], path, values)

    def delete_record(self, key: Any) -> None:
        flag = False
        data = read_from_json("db_files/db.json")
        num_of_files = data[self.name]['num of files']
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}_{file_num + 1}.json")
            for k in file_data.keys():
                if k == str(key):
                    flag = True
                    del_key = k
            if flag == False:
                raise ValueError
            del file_data[del_key]
        data[self.name]['num of lines'] -= 1
        write_to_json(f"db_files/{self.name}_{file_num + 1}.json", file_data)
        write_to_json("db_files/db.json", data)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        flag = False
        db = read_from_json("db_files/db.json")
        num_of_files = db[self.name]['num of files']
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}_{file_num + 1}.json")
            for k in file_data.keys():
                if k == str(key):
                    flag = True
                    return file_data[k]
        raise ValueError

        # write_to_json(f"db_files/{self.name}{file_num + 1}.json", file_data)
        # write_to_json("db_files/db.json", db)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        flag = None
        db = read_from_json("db_files/db.json")
        primary_key = db[self.name]['primary key']
        try:
            flag = self.get_record(values[primary_key])
        except:
            new_record = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}
            if db[self.name]['num of lines'] % 10 == 0 and db[self.name]['num of lines'] != 0:
                db[self.name]['num of files'] += 1
                write_to_json(f"db_files/{self.name}{db[self.name]['num of files']}.json", new_record)
            else:
                add_to_json(f"db_files/{self.name}{db[self.name]['num of files']}.json", new_record)
            db[self.name]['num of lines'] += 1
            write_to_json("db_files/db.json", db)
            # update_indexes(db,values,self.name)
        if flag is not None:
            raise ValueError


    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        index_tree = BPlusTree(f'db_files/{self.name}_order_by_{field_to_index}.db', order=50)
        db_data = read_from_json("db_files/db.json")
        if field_to_index in db_data[self.name]['indexes']:
            return
        num_of_files = db_data[self.name]['num of files']
        primary_key = db_data[self.name]['primary key']
        if primary_key == field_to_index:

            for file_num in range(num_of_files):
                # print(file_num)
                path = f"db_files/{self.name}_{file_num + 1}.json"
                file_data = read_from_json(path)

                for k in file_data.keys():
                    print(k)
                    index_tree[k] = path
        else:
            for file_num in range(num_of_files):
                path = f"db_files/{self.name}_{file_num + 1}.json"
                file_data = read_from_json(path)

                for v in file_data.values():
                    print(v)
                    index_tree[v[field_to_index]] = path
        index_tree.close()
        db_data[self.name]['indexes'] += field_to_index

class DataBase:
    # Put here any instance information needed to support the API
    def __init__(self):
        if not os.path.isfile("db_files/db.json"):
            write_to_json("db_files/db.json", {"num of tables": 0})

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        data = read_from_json("db_files/db.json")
        if table_name in data.keys():
            raise NameError
        write_to_json(f"db_files/{table_name}_1.json", {})
        data['num of tables'] += 1
        data[table_name] = \
            {'num of files': 1,
             'num of lines': 0,
             'fields': convert_from_DBfield_to_dict(fields),
             'primary key': key_field_name,
             'indexes': []
             }
        write_to_json("db_files/db.json", data)

        table = DBTable(table_name, fields, key_field_name)
        table.create_index(key_field_name)

        return table

    def num_tables(self) -> int:
        db_data = read_from_json("db_files/db.json")
        return db_data['num of tables']

    def get_table(self, table_name: str) -> DBTable:
        db_data = read_from_json("db_files/db.json")
        return DBTable(table_name, convert_from_dict_to_DBfield(db_data[table_name]['fields']),
                       db_data[table_name]['primary key'])

    def delete_table(self, table_name: str) -> None:
        num = delete_table_from_db(table_name)
        delete_files_of_table(table_name, num)

    def get_tables_names(self) -> List[Any]:
        data = read_from_json("db_files/db.json")
        return [key for key in data.keys() if key != 'num of tables']

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError



