from flask import Flask, jsonify, request
import sqlite3
import os
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

# ... (rest of configuration)

@app.route('/appwrite-data', methods=['GET', 'POST'])
def get_appwrite_data():
    try:
        # Handle POST parameters
        if request.method == 'POST':
            try:
                data = request.get_json(force=True) or {}
            except:
                data = {}
        else:
            data = request.args
            
        client = get_appwrite_client()
        databases = Databases(client)
        
        # Build Queries
        queries = []
        
        # Map of supported filter keys
        valid_filters = [
            'employeeId', 'firstName', 'lastName', 
            'department', 'fullTime', 'hireDate'
        ]
        
        for key in valid_filters:
            if data.get(key):
                queries.append(Query.equal(key, data[key]))
        
        # Special handling for Salary (if min/max provided, or exact)
        if data.get('salary'):
            queries.append(Query.equal('salary', data['salary']))

        # Date Range Filtering for hireDate
        if data.get('hireDateStart'):
            queries.append(Query.greater_than_equal('hireDate', data['hireDateStart']))
        
        if data.get('hireDateEnd'):
            queries.append(Query.less_than_equal('hireDate', data['hireDateEnd']))
            
        result = databases.list_documents(
            database_id=APPWRITE_DB_ID,
            collection_id=APPWRITE_COLLECTION_ID,
            queries=queries
        )
        
        documents = result['documents']
        processed_docs = []
        
        for doc in documents:
            # Create a clean dictionary removing internal Appwrite fields if desired
            # or keep them. Here we just process/mask as per original logic if needed
            
            # Example masking logic from original code applied to this new data
            # Adjust field names based on actual schema
            clean_doc = doc.copy()
            
            # Mask sensitive fields
            for key in clean_doc.keys():
                # Remove internal fields starting with $ if you want to clean up response
                # if key.startswith('$'): continue 
                
                key_lower = key.lower().replace(' ', '_').replace('.', '')
                if key_lower in ['employee_code', 'employeecode', 'employeeid']:
                    clean_doc[key] = mask_field(clean_doc[key])
                elif key_lower in ['first_name', 'firstname']:
                    clean_doc[key] = mask_field(clean_doc[key])
                elif key_lower in ['middle_name', 'middlename']:
                    clean_doc[key] = mask_field(clean_doc[key])
                elif key_lower in ['last_name', 'lastname']:
                    clean_doc[key] = mask_field(clean_doc[key])
                elif key_lower in ['pan_no', 'panno', 'pan_number']:
                    clean_doc[key] = mask_field(clean_doc[key])
            
            processed_docs.append(clean_doc)
            
        return jsonify({
            'total': result['total'],
            'documents': processed_docs
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/employees', methods=['GET', 'POST'])
def get_employees():
    try:
        # Get filter parameters from request body or query params
        if request.method == 'POST':
            try:
                data = request.get_json(force=True) or {}
            except:
                data = {}
        else:
            data = request.args
            
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        resignation_date = data.get('resignation_date')
        
        # Connect to SQLite database
        conn = sqlite3.connect('Chinook.db')
        cursor = conn.cursor()
        
        # Build dynamic query with filters
        query = "SELECT * FROM Employee_Demo WHERE 1=1"
        params = []
        
        if start_date and end_date:
            query += " AND date([date of joining]) BETWEEN date(?) AND date(?)"
            params.extend([start_date, end_date])
            
        if resignation_date:
            query += " AND date([date of Resignation]) = date(?)"
            params.append(resignation_date)
        
        # Execute query
        cursor.execute(query, params)
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        employees = []
        for row in rows:
            employee = dict(zip(columns, row))
            
            # Mask sensitive fields - check all possible column name variations
            for key in employee.keys():
                if key.lower().replace(' ', '_').replace('.', '') in ['employee_code', 'employeecode']:
                    employee[key] = mask_field(employee[key])
                elif key.lower().replace(' ', '_').replace('.', '') in ['first_name', 'firstname']:
                    employee[key] = mask_field(employee[key])
                elif key.lower().replace(' ', '_').replace('.', '') in ['middle_name', 'middlename']:
                    employee[key] = mask_field(employee[key])
                elif key.lower().replace(' ', '_').replace('.', '') in ['last_name', 'lastname']:
                    employee[key] = mask_field(employee[key])
                elif key.lower().replace(' ', '_').replace('.', '') in ['pan_no', 'panno', 'pan_number']:
                    employee[key] = mask_field(employee[key])
                
            employees.append(employee)
        
        cursor.close()
        conn.close()
        
        return jsonify(employees)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)