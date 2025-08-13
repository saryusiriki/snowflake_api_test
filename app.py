import os
import snowflake.connector
from flask import Flask, request, jsonify

app = Flask(__name__)
@app.route("/")
def home():
    return "Hello , started Snowflake API"

def connect_to_snowflake():
    return snowflake.connector.connect(
        user='SARYU',
        password='Saryu@12345678',
        account='UDQXAND-WHA82584',
        database='TEST',
        schema='TEST',
        warehouse='COMPUTE_WH'
    )

@app.route("/query", methods=["POST"])
def run_query():
    data = request.get_json()
    query = data.get("query")
    if not query:
        return jsonify({"status": "error", "message": "No query provided"}), 400

    conn = connect_to_snowflake()
    cur = conn.cursor()

    try:
        cur.execute(query)
        if cur.description is None:
            return jsonify({"status": "success", "data": []})
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return jsonify([dict(zip(columns, row)) for row in results])
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)