#Handles low-level file I/O operations for table data persistence.

import json
import os
from typing import Dict, List, Any, Optional


class StorageEngine:
       
    def __init__(self, data_dir: str = "data"):
       
        self.data_dir = data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _get_table_path(self, table_name: str) -> str:
        
        return os.path.join(self.data_dir, f"{table_name}.json")
    
    def table_exists(self, table_name: str) -> bool:
        
        return os.path.exists(self._get_table_path(table_name))
    
    def create_table_file(self, table_name: str) -> None:
        
        table_path = self._get_table_path(table_name)
        if os.path.exists(table_path):
            raise FileExistsError(f"Table '{table_name}' already exists")
        
        with open(table_path, 'w') as f:
            json.dump([], f, indent=2)
    
    def read_table(self, table_name: str) -> List[Dict[str, Any]]:
        
        table_path = self._get_table_path(table_name)
        
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table '{table_name}' does not exist")
        
        try:
            with open(table_path, 'r') as f:
                data = json.load(f)
                # Defensive check that ensures file contains valid list
                if not isinstance(data, list):
                    raise ValueError(f"Corrupted table file: {table_name}")
                return data
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in table '{table_name}': {e}")
    
    def write_table(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        
        table_path = self._get_table_path(table_name)
        temp_path = table_path + ".tmp"
        
        try:
            # Write to temporary file
            with open(temp_path, 'w') as f:
                json.dump(rows, f, indent=2)
            
           
            os.replace(temp_path, table_path)
            
        except Exception as e:
            # Cleanup temp file on failure
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise IOError(f"Failed to write table '{table_name}': {e}")
    
    def delete_table_file(self, table_name: str) -> None:
       
        table_path = self._get_table_path(table_name)
        if os.path.exists(table_path):
            os.remove(table_path)
        else:
            raise FileNotFoundError(f"Table '{table_name}' does not exist")