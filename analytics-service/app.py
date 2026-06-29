import sys
import time
import os
import json
import pika
import redis
from prometheus_client import start_http_server, Counter, Gauge
import traceback

print("Starting analytics service...", flush=True)

order_counter = Counter('orders_total', 'Total number of orders')
avg_price_gauge = Gauge('average_order_price', 'Average price of orders')

try:
    r = redis.Redis.from_url(os.environ.get('REDIS_URL', 'redis://redis:6379'))
    r.ping()
    print("Connected to Redis", flush=True)
except Exception as e:
    print(f"Error connecting to Redis: {e}", file=sys.stderr, flush=True)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

def callback(ch, method, properties, body):
    try:
        print(f"Received message: {body}", flush=True)
        order = json.loads(body)
        print(f"Processing order: {order}", flush=True)
        order_counter.inc()
        total = r.incr('total_orders')
        sum_prices = r.incrbyfloat('sum_prices', order['price'])
        avg = sum_prices / total
        r.set('avg_price', avg)
        avg_price_gauge.set(avg)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Order processed, total: {total}, avg: {avg}", flush=True)
    except Exception as e:
        print(f"Callback error: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    max_retries = 20
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt+1} to connect to RabbitMQ...", flush=True)
            params = pika.URLParameters(os.environ.get('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672'))
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue='orders', durable=False)
            channel.basic_consume(queue='orders', on_message_callback=callback, auto_ack=False)
            print("Connected to RabbitMQ, starting analytics service on port 8001", flush=True)
            start_http_server(8001)
            channel.start_consuming()
            return
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay}s...", flush=True)
                time.sleep(retry_delay)
            else:
                print("Max retries exceeded, exiting.", file=sys.stderr, flush=True)
                sys.exit(1)

if __name__ == '__main__':
    main()