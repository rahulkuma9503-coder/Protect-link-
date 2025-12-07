import os
import sqlite3
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

DB_NAME = "links.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/join')
def join_page():
    """Serves the HTML page for the Telegram Web App."""
    token = request.args.get('token')
    if not token:
        return "Error: No token provided.", 400
    # The token is passed to the HTML template to be used by JavaScript
    return render_template('join.html', token=token)

@app.route('/getgrouplink/<token>')
def get_group_link(token):
    """API endpoint for the Web App to fetch the real group link."""
    with get_db_connection() as conn:
        link_data = conn.execute("SELECT group_link FROM protected_links WHERE id = ?", (token,)).fetchone()
    
    if link_data:
        return jsonify({"url": link_data["group_link"]})
    else:
        return jsonify({"error": "Link not found"}), 404

if __name__ == '__main__':
    # This part is for local testing. Render uses Gunicorn from start.sh
    app.run(port=int(os.environ.get('PORT', 5000)))
