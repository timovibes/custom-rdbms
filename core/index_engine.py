# Implements in-memory hash-based indexing for O(1) primary key lookups.
# hashmaps is used because of its simple implementation and sufficient for quality serches.

from typing import Dict, Any, Optional, List


class IndexEngine:
    
    def __init__(self):
       
        self.indexes: Dict[str, Dict[Any, Dict[str, Any]]] = {}
    
    def build_index(
        self, 
        table_name: str, 
        rows: List[Dict[str, Any]], 
        primary_key: str
    ) -> None:
        
        index = {}
        
        for row in rows:
            pk_value = row.get(primary_key)
            
            if pk_value is None:
                raise ValueError(
                    f"Row missing primary key '{primary_key}': {row}"
                )
            
            # Detect duplicate primary keys
            if pk_value in index:
                raise ValueError(
                    f"Duplicate primary key '{pk_value}' in table '{table_name}'"
                )
            
            
            index[pk_value] = row
        
        self.indexes[table_name] = index
    
    def lookup(
        self, 
        table_name: str, 
        primary_key_value: Any
    ) -> Optional[Dict[str, Any]]:
       
        if table_name not in self.indexes:
            return None
        
        return self.indexes[table_name].get(primary_key_value)
    
    def insert(
        self, 
        table_name: str, 
        row: Dict[str, Any], 
        primary_key: str
    ) -> None:
       
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        pk_value = row[primary_key]
        
        if pk_value in self.indexes[table_name]:
            raise ValueError(
                f"Primary key '{pk_value}' already exists in table '{table_name}'"
            )
        
        self.indexes[table_name][pk_value] = row
    
    def delete(
        self, 
        table_name: str, 
        primary_key_value: Any
    ) -> None:
       
        if table_name in self.indexes:
            self.indexes[table_name].pop(primary_key_value, None)
    
    def update(
        self, 
        table_name: str, 
        old_pk_value: Any, 
        new_row: Dict[str, Any], 
        primary_key: str
    ) -> None:
        
        self.delete(table_name, old_pk_value)
        self.insert(table_name, new_row, primary_key)
    
    def drop_index(self, table_name: str) -> None:
       
        if table_name in self.indexes:
            del self.indexes[table_name]
    
    def has_index(self, table_name: str) -> bool:
        
        return table_name in self.indexes