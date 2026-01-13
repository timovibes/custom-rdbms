"""
REPL (Read-Eval-Print Loop) is an interactive command line interface with SQL-like syntax parsing using Regex.
It is lightweight and shows how pattern matching

"""

import re
from typing import Dict, Any, Optional
from core.storage_engine import StorageEngine
from core.schema_manager import SchemaManager
from core.execution_engine import ExecutionEngine


class REPL:
    
    def __init__(self):
        # Initializes REPL with execution engine.
        self.storage = StorageEngine()
        self.schema = SchemaManager()
        self.executor = ExecutionEngine(self.storage, self.schema)
        self._load_all_indexes()
    
    def _load_all_indexes(self) -> None:

        # Loads indexes for all existing tables at startup because indexes are in-memory only
        try:
            schema = self.schema.load_schema()
            for table_name in schema:
                try:
                    self.executor.load_table_index(table_name)
                except Exception as e:
                    print(f"Warning: Could not load index for '{table_name}': {e}")
        except Exception:
            pass
    
    def parse_create_table(self, sql: str) -> Optional[Dict[str, Any]]:
       
        pattern = r'CREATE TABLE (\w+) \((.*?)\)(?:\s+PRIMARY KEY (\w+))?'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        table_name = match.group(1)
        columns_str = match.group(2)
        primary_key = match.group(3)
        
        # Parse column definitions
        columns = {}
        for col_def in columns_str.split(','):
            parts = col_def.strip().split()
            if len(parts) != 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            col_name, col_type = parts
            columns[col_name] = col_type
        
        return {
            'table': table_name,
            'columns': columns,
            'primary_key': primary_key
        }
    
    def parse_insert(self, sql: str) -> Optional[Dict[str, Any]]:
        
        pattern = r'INSERT INTO (\w+) VALUES \((.*?)\)'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # Parse values. It handles strings in quotes, numbers, booleans
        values = []
        
        for value in re.findall(r'"([^"]*)"|\'([^\']*)\'|([^,]+)', values_str):
           
            val = value[0] or value[1] or value[2]
            val = val.strip()
            
            if val.lower() == 'true':
                values.append(True)
            elif val.lower() == 'false':
                values.append(False)
            elif val.isdigit() or (val[0] == '-' and val[1:].isdigit()):
                values.append(int(val))
            elif self._is_float(val):
                values.append(float(val))
            else:
                values.append(val)
        
        return {
            'table': table_name,
            'values': values
        }
    
    def _is_float(self, s: str) -> bool:
       
        try:
            float(s)
            return '.' in s
        except ValueError:
            return False
    
    def parse_select(self, sql: str) -> Optional[Dict[str, Any]]:
        
        pattern = r'SELECT (.*?) FROM (\w+)(?:\s+WHERE (.+))?'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_str = match.group(3)
        
        # Parse columns
        columns = None if columns_str == '*' else [
            c.strip() for c in columns_str.split(',')
        ]
        
        # Parse WHERE clause
        where_func = None
        if where_str:
            where_func = self._parse_where_clause(where_str)
        
        return {
            'table': table_name,
            'columns': columns,
            'where': where_func
        }
    
    def parse_delete(self, sql: str) -> Optional[Dict[str, Any]]:
        
        pattern = r'DELETE FROM (\w+) WHERE (.+)'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        table_name = match.group(1)
        where_str = match.group(2)
        
        return {
            'table': table_name,
            'where': self._parse_where_clause(where_str)
        }
    
    def parse_update(self, sql: str) -> Optional[Dict[str, Any]]:
        
        pattern = r'UPDATE (\w+) SET (.+?) WHERE (.+)'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        table_name = match.group(1)
        set_str = match.group(2)
        where_str = match.group(3)
        
        # Parse SET clause
        updates = {}
        for assignment in set_str.split(','):
            col, val = assignment.split('=')
            col = col.strip()
            val = val.strip().strip('"\'')
            
           
            if val.isdigit():
                val = int(val)
            elif self._is_float(val):
                val = float(val)
            elif val.lower() == 'true':
                val = True
            elif val.lower() == 'false':
                val = False
            
            updates[col] = val
        
        return {
            'table': table_name,
            'updates': updates,
            'where': self._parse_where_clause(where_str)
        }
    
    def parse_join(self, sql: str) -> Optional[Dict[str, Any]]:
        
        pattern = r'JOIN (\w+)\s*,\s*(\w+) ON (\w+)'
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        
        if not match:
            return None
        
        return {
            'left_table': match.group(1),
            'right_table': match.group(2),
            'on_column': match.group(3)
        }
    
    def _parse_where_clause(self, where_str: str) -> callable:
       
        # Parse condition: column operator value
        pattern = r'(\w+)\s*(=|>|<|>=|<=|!=)\s*(.+)'
        match = re.match(pattern, where_str.strip())
        
        if not match:
            raise ValueError(f"Invalid WHERE clause: {where_str}")
        
        column = match.group(1)
        operator = match.group(2)
        value_str = match.group(3).strip().strip('"\'')
        
        
        if value_str.isdigit():
            value = int(value_str)
        elif self._is_float(value_str):
            value = float(value_str)
        elif value_str.lower() == 'true':
            value = True
        elif value_str.lower() == 'false':
            value = False
        else:
            value = value_str
        
        # Build lambda function because it is Compact and fits functional programming style of filters
        if operator == '=':
            return lambda row: row.get(column) == value
        elif operator == '>':
            return lambda row: row.get(column) > value
        elif operator == '<':
            return lambda row: row.get(column) < value
        elif operator == '>=':
            return lambda row: row.get(column) >= value
        elif operator == '<=':
            return lambda row: row.get(column) <= value
        elif operator == '!=':
            return lambda row: row.get(column) != value
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    def execute(self, sql: str) -> Any:
        
        sql = sql.strip()
        
        # Route to appropriate parser
        if sql.upper().startswith('CREATE TABLE'):
            params = self.parse_create_table(sql)
            self.executor.create_table(
                params['table'], 
                params['columns'],
                params.get('primary_key')
            )
            return f"Table '{params['table']}' created successfully"
        
        elif sql.upper().startswith('INSERT INTO'):
            params = self.parse_insert(sql)
            # Convert positional values to named row
            table_schema = self.schema.get_table_schema(params['table'])
            columns = list(table_schema['columns'].keys())
            row = dict(zip(columns, params['values']))
            self.executor.insert_row(params['table'], row)
            return "1 row inserted"
        
        elif sql.upper().startswith('SELECT'):
            params = self.parse_select(sql)
            rows = self.executor.select_rows(
                params['table'],
                where=params['where'],
                columns=params['columns']
            )
            return rows
        
        elif sql.upper().startswith('DELETE'):
            params = self.parse_delete(sql)
            count = self.executor.delete_rows(
                params['table'],
                params['where']
            )
            return f"{count} row(s) deleted"
        
        elif sql.upper().startswith('UPDATE'):
            params = self.parse_update(sql)
            count = self.executor.update_rows(
                params['table'],
                params['where'],
                params['updates']
            )
            return f"{count} row(s) updated"
        
        elif sql.upper().startswith('JOIN'):
            params = self.parse_join(sql)
            rows = self.executor.nested_loop_join(
                params['left_table'],
                params['right_table'],
                params['on_column']
            )
            return rows
        
        elif sql.upper().startswith('DROP TABLE'):
            table_name = sql.split()[2]
            self.executor.drop_table(table_name)
            return f"Table '{table_name}' dropped"
        
        else:
            raise ValueError(f"Unsupported command: {sql}")
    
    def run(self) -> None:
        
        print("=" * 60)
        print("Custom RDBMS - Interactive Shell")
        print("=" * 60)
        print("Commands: CREATE TABLE, INSERT, SELECT, DELETE, UPDATE, JOIN")
        print("Type 'exit' to quit\n")
        
        while True:
            try:
                # Read command
                sql = input("rdbms> ").strip()
                
                if sql.lower() == 'exit':
                    print("Goodbye!")
                    break
                
                if not sql:
                    continue
                
                # Execute command
                result = self.execute(sql)
                
                # Print result
                if isinstance(result, list):
                    if result:
                        # Print as formatted table
                        self._print_table(result)
                    else:
                        print("0 rows returned")
                else:
                    print(result)
                
                print()  
                
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}\n")
    
    def _print_table(self, rows: list) -> None:
        
        if not rows:
            return
        
        # Get all column names
        columns = list(rows[0].keys())
        
        # Calculate column widths
        widths = {col: len(col) for col in columns}
        for row in rows:
            for col in columns:
                widths[col] = max(widths[col], len(str(row.get(col, ''))))
        
        # Print header
        header = ' | '.join(col.ljust(widths[col]) for col in columns)
        print(header)
        print('-' * len(header))
        
        # Print rows
        for row in rows:
            row_str = ' | '.join(
                str(row.get(col, '')).ljust(widths[col]) 
                for col in columns
            )
            print(row_str)
        
        print(f"\n{len(rows)} row(s) returned")


def main():
    
    repl = REPL()
    repl.run()


if __name__ == '__main__':
    main()