#Manages table schemas in a centralized master_schema.json file.

import json
import os
from typing import Dict, List, Any, Optional


class SchemaManager:
       
    def __init__(self, schema_path: str = "data/master_schema.json"):
        
        self.schema_path = schema_path
        self._ensure_schema_file()
    
    def _ensure_schema_file(self) -> None:
        
        if not os.path.exists(self.schema_path):
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(self.schema_path), exist_ok=True)
            with open(self.schema_path, 'w') as f:
                json.dump({}, f, indent=2)
    
    def load_schema(self) -> Dict[str, Any]:
        
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Corrupted schema file: {e}")
    
    def save_schema(self, schema: Dict[str, Any]) -> None:
        
        temp_path = self.schema_path + ".tmp"
        
        try:
            with open(temp_path, 'w') as f:
                json.dump(schema, f, indent=2)
            os.replace(temp_path, self.schema_path)
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise IOError(f"Failed to save schema: {e}")
    
    def create_table_schema(
        self, 
        table_name: str, 
        columns: Dict[str, str], 
        primary_key: Optional[str] = None
    ) -> None:
        
        schema = self.load_schema()
        
        if table_name in schema:
            raise ValueError(f"Table '{table_name}' already exists in schema")
        
        # Validate primary key exists in columns
        if primary_key and primary_key not in columns:
            raise ValueError(
                f"Primary key '{primary_key}' not found in columns"
            )
        
        # Validate data types
        valid_types = {"Int", "String", "Float", "Bool"}
        for col, dtype in columns.items():
            if dtype not in valid_types:
                raise ValueError(
                    f"Invalid data type '{dtype}' for column '{col}'. "
                    f"Valid types: {valid_types}"
                )
        
        schema[table_name] = {
            "columns": columns,
            "primary_key": primary_key
        }
        
        self.save_schema(schema)
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
       
        schema = self.load_schema()
        
        if table_name not in schema:
            raise ValueError(f"Table '{table_name}' not found in schema")
        
        return schema[table_name]
    
    def drop_table_schema(self, table_name: str) -> None:
        
        schema = self.load_schema()
        
        if table_name not in schema:
            raise ValueError(f"Table '{table_name}' not found in schema")
        
        del schema[table_name]
        self.save_schema(schema)
    
    def table_exists(self, table_name: str) -> bool:
       
        schema = self.load_schema()
        return table_name in schema
    
    def validate_row(self, table_name: str, row: Dict[str, Any]) -> None:
        
        table_schema = self.get_table_schema(table_name)
        columns = table_schema["columns"]
        
        # Check for missing columns
        for col in columns:
            if col not in row:
                raise ValueError(f"Missing required column: {col}")
        
        # Check for extra columns
        for col in row:
            if col not in columns:
                raise ValueError(f"Unknown column: {col}")
        
        
        for col, expected_type in columns.items():
            value = row[col]
            
            if expected_type == "Int":
                if not isinstance(value, int) or isinstance(value, bool):
                    raise ValueError(
                        f"Column '{col}' expects Int, got {type(value).__name__}"
                    )
            elif expected_type == "String":
                if not isinstance(value, str):
                    raise ValueError(
                        f"Column '{col}' expects String, got {type(value).__name__}"
                    )
            elif expected_type == "Float":
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise ValueError(
                        f"Column '{col}' expects Float, got {type(value).__name__}"
                    )
            elif expected_type == "Bool":
                if not isinstance(value, bool):
                    raise ValueError(
                        f"Column '{col}' expects Bool, got {type(value).__name__}"
                    )