const express = require('express');
const http = require('http'); // Import 'http' to create a server
const runConsumer = require('./kafkaConsumer');

const app = express();
const port = 3000;

// Middleware for handling JSON
app.use(express.json());

// Sample route (optional, for testing purposes)
app.get('/', (req, res) => {
  res.send('Kafka Consumer running');
});

// Create the HTTP server instance
const server = http.createServer(app);

// Start Kafka consumer and pass the server instance to it
runConsumer(server).then(() => {
  console.log('Kafka consumer connected and running');
}).catch(error => {
  console.error('Error starting Kafka consumer:', error);
});

// Start the Express app and listen on port 3000
server.listen(port, () => {
  console.log(`Express.js server is running on port ${port}`);
});
