#Execution Engine Module which implements core CRUD operations and relational algebra

from typing import Dict, List, Any, Optional, Callable
from core.storage_engine import StorageEngine
from core.schema_manager import SchemaManager
from core.index_engine import IndexEngine


class ExecutionEngine:
    
    def __init__(self, storage: StorageEngine, schema: SchemaManager):
        
        self.storage = storage
        self.schema = schema
        self.index = IndexEngine()
    
    def create_table(
        self, 
        table_name: str, 
        columns: Dict[str, str], 
        primary_key: Optional[str] = None
    ) -> None:
       
        #Creates schema definition
        self.schema.create_table_schema(table_name, columns, primary_key)
        
        try:
            #Creates physical storage file
            self.storage.create_table_file(table_name)
            
            #Initializes empty index
            if primary_key:
                self.index.build_index(table_name, [], primary_key)
                
        except Exception as e:
            #Removes schema if storage fails
            self.schema.drop_table_schema(table_name)
            raise e
    
    def insert_row(self, table_name: str, row: Dict[str, Any]) -> None:
    
        table_schema = self.schema.get_table_schema(table_name)
        
        #Validate row data types and columns
        self.schema.validate_row(table_name, row)
        
        #Load existing rows
        rows = self.storage.read_table(table_name)
        
        #Check primary key uniqueness
        primary_key = table_schema.get("primary_key")
        if primary_key:
            pk_value = row[primary_key]
            
            # O(1) lookup via index becasuse index is O(1) and scan is O(n) so for 1M rows, index=0.001ms, scan=100ms
            if self.index.lookup(table_name, pk_value):
                raise ValueError(
                    f"Primary key violation: '{pk_value}' already exists"
                )
        
        # Append row
        rows.append(row)
        
        # Persist to disk
        self.storage.write_table(table_name, rows)
        
        # Update index
        if primary_key:
            self.index.insert(table_name, row, primary_key)
    
    def select_rows(
        self, 
        table_name: str, 
        where: Optional[Callable[[Dict[str, Any]], bool]] = None,
        columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        
        rows = self.storage.read_table(table_name)
        
        # Filter rows
        if where:
            rows = [row for row in rows if where(row)]
        
        # Project columns
        if columns:
            rows = [
                {col: row[col] for col in columns if col in row}
                for row in rows
            ]
        
        return rows
    
    def select_by_primary_key(
        self, 
        table_name: str, 
        pk_value: Any
    ) -> Optional[Dict[str, Any]]:
        
        return self.index.lookup(table_name, pk_value)
    
    def delete_rows(
        self, 
        table_name: str, 
        where: Callable[[Dict[str, Any]], bool]
    ) -> int:
        
        rows = self.storage.read_table(table_name)
        table_schema = self.schema.get_table_schema(table_name)
        primary_key = table_schema.get("primary_key")
        
        # Filter out rows that match delete condition
        rows_to_keep = []
        deleted_count = 0
        
        for row in rows:
            if where(row):
                # Delete from index
                if primary_key:
                    self.index.delete(table_name, row[primary_key])
                deleted_count += 1
            else:
                rows_to_keep.append(row)
        
        # Write remaining rows
        self.storage.write_table(table_name, rows_to_keep)
        
        return deleted_count
    
    def update_rows(
        self,
        table_name: str,
        where: Callable[[Dict[str, Any]], bool],
        updates: Dict[str, Any]
    ) -> int:
        
        rows = self.storage.read_table(table_name)
        table_schema = self.schema.get_table_schema(table_name)
        primary_key = table_schema.get("primary_key")
        
        updated_count = 0
        
        for row in rows:
            if where(row):
                old_pk = row.get(primary_key) if primary_key else None
                
                # Apply updates
                for col, value in updates.items():
                    if col in row:
                        row[col] = value
                
                # Validate updated row
                self.schema.validate_row(table_name, row)
                
                # Update index if primary key changed
                if primary_key:
                    new_pk = row[primary_key]
                    if old_pk != new_pk:
                        self.index.update(table_name, old_pk, row, primary_key)
                
                updated_count += 1
        
        self.storage.write_table(table_name, rows)
        return updated_count
    
    def nested_loop_join(
        self,
        left_table: str,
        right_table: str,
        on_column: str
    ) -> List[Dict[str, Any]]:
        
        left_rows = self.storage.read_table(left_table)
        right_rows = self.storage.read_table(right_table)
        
        result = []
        
        for left_row in left_rows:
            for right_row in right_rows:
                # Check if join condition is met
                if left_row.get(on_column) == right_row.get(on_column):
                    # Merge rows (right table overwrites left on conflict)
                    merged = {**left_row, **right_row}
                    result.append(merged)
        
        return result
    
    def drop_table(self, table_name: str) -> None:
        
        # Drop index
        self.index.drop_index(table_name)
        
        # Drop storage file
        self.storage.delete_table_file(table_name)
        
        # Drop schema
        self.schema.drop_table_schema(table_name)
    
    def load_table_index(self, table_name: str) -> None:
       
        table_schema = self.schema.get_table_schema(table_name)
        primary_key = table_schema.get("primary_key")
        
        if primary_key:
            rows = self.storage.read_table(table_name)
            self.index.build_index(table_name, rows, primary_key)