// Importing necessary modules
const express = require('express');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');
const app = express();

// Middleware
app.use(bodyParser.json());

// MySQL connection pool
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'admin12345',
  database: 'task_queue',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Endpoint to add a task to the queue
app.post('/tasks', async (req, res) => {
  const { email } = req.body;
  const id = uuidv4();

  try {
    await pool.query('INSERT INTO tasks (id, email, status) VALUES (?, ?, ?)', [id, email, 'pending']);
    res.status(201).json({ message: 'Task added to the queue', taskId: id });
  } catch (error) {
    console.error('Error adding task:', error);
    res.status(500).json({ message: 'Internal server error' });
  }
});

// Endpoint to fetch tasks and their status
app.get('/tasks', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM tasks');
    res.json(rows);
  } catch (error) {
    console.error('Error fetching tasks:', error);
    res.status(500).json({ message: 'Internal server error' });
  }
});

// Worker function to process tasks
const processTasks = async () => {
  try {
    const [tasks] = await pool.query('SELECT * FROM tasks WHERE status = ?', ['pending']);

    for (const task of tasks) {
      const { id, email } = task;

      // Mark task as in progress
      await pool.query('UPDATE tasks SET status = ? WHERE id = ?', ['in_progress', id]);

      console.log(`Processing task: Sending welcome email to ${email}`);

      // Simulate email sending
      await new Promise((resolve) => setTimeout(resolve, 5000));

      // Mark task as completed
      await pool.query('UPDATE tasks SET status = ? WHERE id = ?', ['completed', id]);

      console.log(`Task completed: Email sent to ${email}`);
    }
  } catch (error) {
    console.error('Error processing tasks:', error);
  }
};

// Start processing tasks at regular intervals
setInterval(processTasks, 10000); // Every 10 seconds

// Start the server
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
