const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';

async function startWorker() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    const queue = 'ticket_created_queue';

    await channel.assertQueue(queue, { durable: true });
    channel.prefetch(1);

    console.log(`Notification Worker listening on queue: ${queue}`);

    channel.consume(queue, (msg) => {
      if (msg !== null) {
        const ticketData = JSON.parse(msg.content.toString());
        console.log('\n==============================');
        console.log('🚨 NEW TICKET NOTIFICATION ALERT 🚨');
        console.log(`Ticket ID  : ${ticketData.id}`);
        console.log(`Title      : ${ticketData.title}`);
        console.log(`Priority   : ${ticketData.priority}`);
        console.log(`Created At : ${ticketData.created_at}`);
        console.log('==============================\n');

        // Acknowledge message processing completion
        channel.ack(msg);
      }
    });
  } catch (error) {
    console.error('RabbitMQ consumer error, retrying in 5s...', error);
    setTimeout(startWorker, 5000);
  }
}

startWorker();