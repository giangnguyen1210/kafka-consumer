const { Kafka } = require('kafkajs');
const socketIo = require('socket.io');

const kafka = new Kafka({
  clientId: 'express-kafka-consumer',
  brokers: ['bimnextnotif-dev.dpunity.com:29092'],
});

const consumer = kafka.consumer({ 
  groupId: 'bimnext-api-giang', 
  autoCommit: false // Disable auto commit
});

// Function to run Kafka consumer
const runConsumer = async (server) => {
  // Initialize Socket.io with the Express server
  const io = socketIo(server, {
    cors: {
      origin: "*", // Replace with your client URL for security
      methods: ["GET", "POST"]
    }
  });

  // Listen for Socket.io connections
  io.on('connection', (socket) => {
    console.log('New client connected:', socket.id);

    socket.on('disconnect', () => {
      console.log('Client disconnected:', socket.id);
    });
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'BIMNEXT.NOTIFY.CHAT_MESSAGE.REQUEST', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        // Process the message
        const messageKey = message.key ? message.key.toString() : 'null';
        const messageValue = message.value ? JSON.parse(message.value.toString()) : null;

        const correlationId = message.headers.correlationId ? message.headers.correlationId.toString() : '';

        console.log(messageValue);
        const chatMessage = {
          topic,
          partition,
          offset: message.offset,
          key: messageKey,
          value: messageValue,
          headers: {
              correlationId: correlationId
          },
        };

        console.log('Processed Kafka message:', messageValue);

        // Emit the message to connected Socket.io clients
        io.emit('newMessage', messageValue);
        
        // After successful message processing, manually commit the offset
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (parseInt(message.offset, 10) + 1).toString(), // Increment the offset by 1
          },
        ]);

        console.log(`Successfully committed offset: ${message.offset}`);
      } catch (error) {
        console.error('Error processing message:', error);
      }
    },
  });
};

module.exports = runConsumer;
