const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

const PORT = process.env.PORT || 3000;
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 26257,
  user: process.env.DB_USER || 'root',
  database: process.env.DB_NAME || 'defaultdb',
});

let channel, connection;

async function connectRabbitMQ() {
  try {
    connection = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    channel = await connection.createChannel();
    await channel.assertQueue('ticket_created_queue', { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('RabbitMQ connection error, retrying in 5s...', error);
    setTimeout(connectRabbitMQ, 5000);
  }
}

async function initDB() {
  let retries = 10;
  while (retries) {
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS tickets (
          id SERIAL PRIMARY KEY,
          title VARCHAR(255) NOT NULL,
          description TEXT NOT NULL,
          priority VARCHAR(50) NOT NULL,
          status VARCHAR(50) DEFAULT 'Open',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      console.log('Database initialized successfully and "tickets" table is ready.');
      return;
    } catch (err) {
      console.log(`Database not ready yet (${err.message}), retrying in 3 seconds...`);
      retries -= 1;
      await new Promise(res => setTimeout(res, 3000));
    }
  }
  console.error('Failed to initialize database after multiple retries.');
}

// Routes
app.post('/api/tickets', async (req, res) => {
  const { title, description, priority } = req.body;
  if (!title || !description || !priority) {
    return res.status(400).json({ error: 'Missing required fields: title, description, priority' });
  }

  try {
    const query = `INSERT INTO tickets (title, description, priority) VALUES ($1, $2, $3) RETURNING *`;
    const result = await pool.query(query, [title, description, priority]);
    const newTicket = result.rows[0];

    if (channel) {
      channel.sendToQueue('ticket_created_queue', Buffer.from(JSON.stringify(newTicket)), { persistent: true });
      console.log(`Published event for ticket ID: ${newTicket.id}`);
    }

    res.status(201).json({ message: 'Ticket created successfully', ticket: newTicket });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/tickets', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM tickets ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, async () => {
  console.log(`Ticket Service running on port ${PORT}`);
  await initDB();
  await connectRabbitMQ();
});