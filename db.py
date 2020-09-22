import json
import os
import operator
from dataclasses import dataclass
from typing import List, Dict, Any
from bplustree import BPlusTree


import json_func
import db_api
#from .db_api import DBField, SelectionCriteria


from dataclasses_json import dataclass_json


ops = {">": operator.gt, "<": operator.lt, "=": operator.eq, ">=": operator.ge, "<=": operator.le,"!=": operator.ne, }


def update_insert_indexes(data, name, values, path):
    for index in data[name]["indexes"]:
        json_data = json_func.read_from_json(f'{db_api.DB_ROOT}/{name}IndexBy{index}.db')
        json_data[str(values[index])] = path
        json_func.write_to_json(f'{db_api.DB_ROOT}/{name}IndexBy{index}.db', json_data)


def update_delete_indexes(data, name,index):
    if index in data[name]["indexes"]:
        json_data = json_func.read_from_json(f'{db_api.DB_ROOT}/{name}IndexBy{index}.db')
        del json_data[str(index)]
        json_func.write_to_json(f'{db_api.DB_ROOT}/{name}IndexBy{index}.db', json_data)


def get_indexes(path, key):
    json_data = json_func.read_from_json(path)
    return json_data[str(key)]


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[db_api.DBField]
    key_field_name: str

    def __init__(self, name, fields, key):
        self. name = name
        self.fields = fields
        self.key_field_name = key

    def count(self) -> int:
        return json_func.read_from_json(f"db_files/db.json")[self.name]["num_of_lines"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        primary_key = data[self.name]["key_field_name"]
        flag = None
        try:
            flag = self.get_record(values[primary_key])
        except ValueError:
            new_record = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}
            if data[self.name]["num_of_lines"] % 10 == 0 and data[self.name]["num_of_lines"] != 0:
                data[self.name]["num_of_files"] += 1
                json_func.write_to_json(f"{db_api.DB_ROOT}/{self.name}{data[self.name]['num_of_files']}.json", new_record)
            else:
                json_func.add_line_to_json(f"db_files/{self.name}{data[self.name]['num_of_files']}.json", values[primary_key] , new_record[values[primary_key]])
            data[self.name]["num_of_lines"] += 1
            json_func.write_to_json(f"{db_api.DB_ROOT}/db.json", data)
            update_insert_indexes(data, self.name, values, f"db_files/{self.name}{data[self.name]['num_of_files']}.json" )
        if flag is not None:
            raise ValueError

    def delete_record(self, key: Any) -> None:
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        primary_key = data[self.name]["key_field_name"]
        json_path = get_indexes(f'{db_api.DB_ROOT}/{self.name}IndexBy{primary_key}.db', key)
        flag = json_func.delete_if_apear(json_path, key)
        if flag == 0:
            raise ValueError
        data[self.name]["num_of_lines"] -= 1
        update_delete_indexes(data, self.name, key)
        json_func.write_to_json(f"{db_api.DB_ROOT}/db.json", data)

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        num_of_files = data[self.name]["num_of_files"]
        primary_key = data[self.name]["key_field_name"]
        for file in range(num_of_files):
            file_data = json_func.read_from_json(f"{db_api.DB_ROOT}/{self.name}{file + 1}.json")
            keys_to_delete = []
            for key, value in file_data.items():
                flag = 0
                for c in criteria:
                    if c.field_name == primary_key:
                        if key in file_data.keys():
                            if not ops[c.operator](int(key), int(c.value)):
                                flag = 1
                                break
                    elif key in file_data.keys():
                        if not ops[c.operator](int(value[c.field_name]), int(c.value)):
                            flag = 1
                            break
                if not flag:
                    keys_to_delete.append(key)
            for key in keys_to_delete:
                del file_data[key]
                data[self.name]["num_of_lines"] -= 1
                update_delete_indexes(data, self.name, key)
            json_func.write_to_json(f"{db_api.DB_ROOT}/{self.name}{file + 1}.json", file_data)
        json_func.write_to_json(f"{db_api.DB_ROOT}/db.json", data)

    def get_record(self, key: Any) -> Dict[str, Any]:
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        num_of_files = data[self.name]["num_of_files"]
        for file in range(num_of_files):
            json_data = json_func.read_from_json(f"{db_api.DB_ROOT}/{self.name}{file + 1}.json")
            for k in json_data.keys():
                if k == str(key):
                    return json_data[k]
        raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        primary_key = data[self.name]["key_field_name"]
        json_path = get_indexes(f'{db_api.DB_ROOT}/{self.name}IndexBy{primary_key}.db', key)
        json_data = json_func.read_from_json(json_path)
        for k in json_data.keys():
            if k == str(key):
                for key, value in values.items():
                    json_data[k][key] = value
                    json_func.write_to_json(json_path, json_data)
                    return None
        raise ValueError

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        num_of_files = data[self.name]["num_of_files"]
        primary_key = data[self.name]["key_field_name"]
        list_to_return = []
        for file in range(num_of_files):
            file_data = json_func.read_from_json(f"{db_api.DB_ROOT}/{self.name}{file + 1}.json")
            for key, value in file_data.items():
                flag = 0
                for c in criteria:
                    if c.field_name == primary_key:
                        if not ops[c.operator](int(key), int(c.value)):
                            flag = 1
                            break
                    elif not ops[c.operator](value[c.field_name], c.value):
                        flag = 1
                        break
                if not flag:
                    list_to_return.append(value)
        return list_to_return

    def create_index(self, field_to_index: str) -> None:
        index = {}
        data = json_func.read_from_json(f"{db_api.DB_ROOT}/db.json")
        if field_to_index in data[self.name]["indexes"]:
            print("index exist")
            return
        data[self.name]["indexes"].append(field_to_index)
        json_func.write_to_json(f"{db_api.DB_ROOT}/db.json", data)

        num_of_files = data[self.name]["num_of_files"]
        primary_key = data[self.name]["key_field_name"]

        if primary_key == field_to_index:
            for file in range(num_of_files):
                path = f"{db_api.DB_ROOT}/{self.name}{file + 1}.json"
                the_file = open(path)
                json_data = json.load(the_file)
                for k in json_data.keys():
                    index[k] = path
        else:
            for file in range(num_of_files):
                path = f"{db_api.DB_ROOT}/{self.name}{file + 1}.json"
                the_file = open(path)
                json_data = json.load(the_file)
                for v in json_data.values():
                    index[v[field_to_index]] = path
        json_func.write_to_json(f'{db_api.DB_ROOT}/{self.name}IndexBy{field_to_index}.db', index)


def convert_from_dbfields(fields):
    fields_names = {}
    for field in fields:
        fields_names[f"{field.name}"] = field.type.__name__
    return fields_names


def str_to_class(field):
    return type(field)


def convert_to_db_fields(args):
    fields = []
    for k, v in args.items():
        fields.append(db_api.DBField(k, str_to_class(v)))
    return fields


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    # Put here any instance information needed to support the API
    def __init__(self):
        if not os.path.isfile("db_files/db.json"):
            json_func.write_to_json("db_files/db.json", {"num_of_tables": 0})

    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str, DB_BACKUP_ROOT=None) -> DBTable:
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        d = {
            "fields": convert_from_dbfields(fields),
            "key_field_name": key_field_name,
            "num_of_lines": 0,
            "num_of_files": 1,
            "indexes": []
        }
        json_func.write_to_json(f"db_files/{table_name}1.json", {})
        json_func.add_table_to_json("db_files/db.json", table_name, d)
        table = DBTable(table_name, fields, key_field_name)
        for field in fields:
            if field.name == key_field_name:
                table.create_index(field.name)
        return table

    def num_tables(self) -> int:
        return json_func.read_from_json("db_files/db.json")["num_of_tables"]

    def get_table(self, table_name: str) -> DBTable:
        data = json_func.read_from_json("db_files/db.json")[table_name]
        return DBTable(table_name, convert_to_db_fields(data["fields"]), data["key_field_name"])

    def delete_table(self, table_name: str) -> None:
        num_of_files = json_func.delete_table_from_json(table_name)
        for file in range(num_of_files):
            path = f"db_files/{table_name}{file + 1}.json"
            os.remove(path)
        data = json_func.read_from_json(f"db_files/db.json")
        data["num_of_tables"] -= 1
        json_func.write_to_json(f"db_files/db.json",data)

    def get_tables_names(self) -> List[Any]:
        data = json_func.read_from_json("db_files/db.json")
        return [name for name in data.keys() if name != "num_of_tables"]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
