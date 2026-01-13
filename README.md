# Custom RDBMS - Pesapal Junior Dev Challenge '26
This is a Relational Database Management System built from scratch in Python 3, demonstrating deep systems thinking and software engineering principles.

## Design Decisions
### 1. **Storage Architecture: Flat-File JSON**
Each table is stored as a separate JSON file (`users.json`, `orders.json`, etc.) and this was chosed because json files reduces development overhead and allowed for a human-readable data format for easier auditing.

### 2. **Schema Management: Centralized Metadata**
There is single `master_schema.json` file stores all table definitions and i decided on this so that there would only be one source of truth.

### 3. **Indexing Strategy: Hash Map for Primary Keys**
I used a hash map because our workload is primary-key lookups, giving O(1) access with far simpler code than a B-tree, and the lack of range queries is an acceptable trade-off.

### 4. **Join Algorithm: Nested Loop Join**
I implemented a nested loop join because the tables are small, it uses constant memory, is far simpler to code than a hash join, and the performance difference is negligible at this scale scale.

### 5. **Query Interface: REPL with Regex Parsing**
I implemented SQL-like parsing using regular expressions because it supports a useful subset of SQL with zero dependencies and far less code

### 6. **Execution Model: Read-Modify-Write**
All operations use a simple Read-Modify-Write model: read the full table from disk, modify in memory, then atomically overwrite the file with os.replace(), trading write amplification and speed for simplicity and crash safety, ideal for tables <10k rows.

### 7. **Web Interface: Minimal Flask App**
Single-file Flask app with inline HTML/CSS for a minimal web interface, chosen for lightweight, no-ORM simplicity, easy single-file deployment, and clear HTTP-to-RDBMS demonstration, with routes for home (GET /), insert (POST /insert), and JSON data (GET /data).

## Quick Start

### Setup
```bash
# Create project structure
mkdir -p custom_rdbms/{core,web,data}
cd custom_rdbms

# Add Python files
touch core/{storage_engine,schema_manager,index_engine,execution_engine}.py
touch core/__init__.py
touch repl.py
touch web/app.py
```

### Run REPL
```bash
python repl.py
```

```sql
rdbms> CREATE TABLE users (id Int, name String, email String) PRIMARY KEY id
rdbms> INSERT INTO users VALUES (1, "Alice", "alice@example.com")
rdbms> SELECT * FROM users WHERE id = 1
```

### Run Web App
```bash
pip install flask
python web/app.py
# Open http://127.0.0.1:5000
```

## AI Assistance & Attribution

In accordance with the challenge guidelines, I utilized **Claude 4.5 Sonnet** and **Gemini 3** as thought partners and coding assistants during this project. 

### How AI was used:
* To generate the initial directory structure and basic Flask routing.
* Consulted AI to verify the logic of the Nested Loop Join and to compare the efficiency of different indexing strategies.
* AI assisted in drafting the technical explanations for architectural trade-offs.

### Changes:
* I manually reviewed and debugged all AI-generated logic for the storage engine to ensure data persistence worked as intended.
* I modified the AI's suggested Regex patterns to better handle specific SQL-like edge cases required for this challenge.
* I have personally tested every line of code. While AI assisted in the process, I take full ownership of the system's architecture and performance.

## License
MIT License - Built for Pesapal Junior Dev Challenge '26

## Acknowledgments
This RDBMS demonstrates that with careful engineering and clear thinking, complex systems can be built without relying on external libraries. Every line of code teaches a concept from database theory.

**Goal Achieved:** A working RDBMS that proves deep systems understanding. 🎯
