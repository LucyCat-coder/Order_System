from flask import Flask, request, jsonify
import psycopg2
import os
import pika
import json
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)
metrics = PrometheusMetrics(app)
metrics.info('app_info', 'Order Service', version='1.0')

# Подключение к БД
def get_db():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    return conn

# Инициализация таблицы
with get_db() as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, item TEXT, price FLOAT, status TEXT)")
        conn.commit()
        
# Подключение к RabbitMQ
params = pika.URLParameters(os.environ['RABBITMQ_URL'])
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='orders')

@app.route('/order', methods=['POST'])
def create_order():
    data = request.json
    item = data['item']
    price = data['price']
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO orders (item, price, status) VALUES (%s, %s, %s) RETURNING id", (item, price, 'pending'))
        order_id = cur.fetchone()[0]
        conn.commit()
    # Отправить событие в очередь
    channel.basic_publish(exchange='', routing_key='orders', body=json.dumps({'id': order_id, 'item': item, 'price': price}))
    return jsonify({'id': order_id, 'status': 'pending'})

@app.route('/order/<int:order_id>')
def get_order(order_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, item, price, status FROM orders WHERE id = %s", (order_id,))
        row = cur.fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify({'id': row[0], 'item': row[1], 'price': row[2], 'status': row[3]})

@app.route('/metrics')  # уже есть через PrometheusMetrics
def metrics_endpoint():
    return app.response_class(response=metrics.generate_latest(), mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)