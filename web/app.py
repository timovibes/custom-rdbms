"""
This is a minimal web interface demonstrating the CRUD operations over HTTP

I mainly used flask for its simplicity and ease of setup.
"""

from flask import Flask, request, render_template_string, jsonify, redirect, url_for
import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.storage_engine import StorageEngine
from core.schema_manager import SchemaManager
from core.execution_engine import ExecutionEngine

app = Flask(__name__)

# initialization of core components
storage = StorageEngine()
schema = SchemaManager()
executor = ExecutionEngine(storage, schema)


# HTML Template with inline CSS for simplicity
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Custom RDBMS Web Interface</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1000px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
        }
        .container {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        h1 {
            color: #667eea;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }
        h2 {
            color: #764ba2;
            margin-top: 30px;
        }
        form {
            background: #f9f9f9;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }
        input[type="text"], input[type="email"], input[type="number"] {
            width: 100%;
            padding: 10px;
            margin: 8px 0;
            border: 2px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
            font-size: 14px;
        }
        input[type="text"]:focus, input[type="email"]:focus, input[type="number"]:focus {
            border-color: #667eea;
            outline: none;
        }
        button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px 30px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            margin-top: 10px;
        }
        button:hover {
            opacity: 0.9;
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
        }
        th {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 12px;
            text-align: left;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }
        tr:hover {
            background: #f5f5f5;
        }
        .message {
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
            background: #d4edda;
            border: 1px solid #c3e6cb;
            color: #155724;
        }
        .error {
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
        }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            background: #667eea;
            color: white;
            border-radius: 12px;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Custom RDBMS Web Interface</h1>
        <p>Built from scratch with Python - No SQLite, No SQLAlchemy</p>
        <span class="badge">Pesapal Junior Dev Challenge '26</span>
        
        {% if message %}
        <div class="message {% if error %}error{% endif %}">
            {{ message }}
        </div>
        {% endif %}
        
        <h2>Insert New User</h2>
        <form method="POST" action="/insert">
            <input type="number" name="id" placeholder="User ID (Integer)" required>
            <input type="text" name="name" placeholder="Full Name" required>
            <input type="email" name="email" placeholder="Email Address" required>
            <button type="submit">Add User</button>
        </form>
        
        <h2>Registered Users</h2>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Email</th>
                </tr>
            </thead>
            <tbody>
                {% if users %}
                    {% for user in users %}
                    <tr>
                        <td>{{ user.id }}</td>
                        <td>{{ user.name }}</td>
                        <td>{{ user.email }}</td>
                    </tr>
                    {% endfor %}
                {% else %}
                    <tr>
                        <td colspan="3" style="text-align: center; color: #999;">
                            No users yet. Add one above!
                        </td>
                    </tr>
                {% endif %}
            </tbody>
        </table>
        
        <p style="margin-top: 30px; color: #999; font-size: 14px;">
            Data is persisted in <code>data/users.json</code> using custom flat-file storage engine.
        </p>
    </div>
</body>
</html>
"""


def initialize_demo_table():
    try:
        #checks if table exists
        schema.get_table_schema('users')
    except ValueError:
        #if the table doesn't exist it is created
        executor.create_table(
            'users',
            columns={
                'id': 'Int',
                'name': 'String',
                'email': 'String'
            },
            primary_key='id'
        )
        print("✓ Initialized 'users' table")


@app.route('/', methods=['GET'])
def index():
    message = request.args.get('message', '')
    error = request.args.get('error', '')
    
    #fetch all users
    try:
        users = executor.select_rows('users')
    except Exception as e:
        users = []
        error = str(e)
    
    return render_template_string(
        HTML_TEMPLATE,
        users=users,
        message=message,
        error=error
    )


@app.route('/insert', methods=['POST'])
def insert():
    try:
        # Extract form data
        user_id = int(request.form['id'])
        name = request.form['name']
        email = request.form['email']
        
        # Insert into database
        executor.insert_row('users', {
            'id': user_id,
            'name': name,
            'email': email
        })

        # USE REDIRECT INSTEAD OF SCRIPT TAGS
        return redirect(url_for('index', message=f'User "{name}" added successfully!'))
        
    except Exception as e:
        # Redirect with the error message in the URL
        return redirect(url_for('index', error=str(e)))


@app.route('/data', methods=['GET'])
def get_data():
    
    try:
        users = executor.select_rows('users')
        return jsonify({
            'success': True,
            'data': users,
            'count': len(users)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/search', methods=['GET'])
def search():
    """
    Search users by ID which demonstrates an indexed lookup
    
    Example: GET /search?id=1
    """
    try:
        user_id = int(request.args.get('id'))
        user = executor.select_by_primary_key('users', user_id)
        
        if user:
            return jsonify({
                'success': True,
                'data': user
            })
        else:
            return jsonify({
                'success': False,
                'error': 'User not found'
            }), 404
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


def main():
   
    print("=" * 60)
    print("Custom RDBMS - Web Interface")
    print("=" * 60)
    
    
    initialize_demo_table()
    
    print("\n🚀 Server starting...")
    print("📍 Open your browser to: http://127.0.0.1:5000")
    print("Press Ctrl+C to stop\n")
    
    app.run(debug=True, host='127.0.0.1', port=5000)


if __name__ == '__main__':
    main()