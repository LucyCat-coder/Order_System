const amqp = require('amqplib');
const express = require('express');
const client = require('prom-client');
const app = express();

const register = new client.Registry();
client.collectDefaultMetrics({ register });

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.listen(3000, () => console.log('Notification service listening on 3000'));

async function connectWithRetry() {
  const maxRetries = 20;
  const retryDelay = 3000;
  for (let i = 0; i < maxRetries; i++) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672');
      console.log('Connected to RabbitMQ');
      const channel = await conn.createChannel();
      // Явно указываем durable: false
      await channel.assertQueue('orders', { durable: false });
      channel.consume('orders', (msg) => {
        try {
          const order = JSON.parse(msg.content.toString());
          console.log(`Sending email for order ${order.id} – item: ${order.item}`);
          channel.ack(msg);
        } catch (e) {
          console.error('Error processing message:', e);
          channel.nack(msg, false, false);
        }
      });
      console.log('Notification service is ready and listening for messages.');
      return;
    } catch (err) {
      console.log(`RabbitMQ not ready, attempt ${i+1}/${maxRetries}, retrying in ${retryDelay/1000}s...`);
      if (err.message) console.error(err.message);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
    }
  }
  console.error('Could not connect to RabbitMQ after multiple attempts');
  process.exit(1);
}

connectWithRetry();