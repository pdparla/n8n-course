const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
  clientId: 'n8n-test-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

// Sample event data
const sampleEvents = [
  {
    event_type: 'customer_feedback',
    event_data: {
      customer_id: 'CUST-12345',
      feedback: 'The product quality is excellent, but delivery was delayed by 3 days.',
      rating: 4
    },
    timestamp: new Date().toISOString()
  },
  {
    event_type: 'order_created',
    event_data: {
      order_id: 'ORD-98765',
      customer_id: 'CUST-54321',
      items: [
        { product_id: 'PROD-001', quantity: 2, price: 29.99 },
        { product_id: 'PROD-042', quantity: 1, price: 149.99 }
      ],
      total_amount: 209.97
    },
    timestamp: new Date().toISOString()
  },
  {
    event_type: 'system_log',
    event_data: {
      level: 'ERROR',
      service: 'payment-gateway',
      message: 'Connection timeout to payment provider after 3 retries',
      error_code: 'PAYMENT_TIMEOUT'
    },
    timestamp: new Date().toISOString()
  }
];

async function sendTestMessages() {
  try {
    await producer.connect();
    console.log('✓ Connected to Kafka');

    const topic = 'ai-workflow-events';

    // Send each sample event
    for (const event of sampleEvents) {
      await producer.send({
        topic,
        messages: [
          {
            key: event.event_type,
            value: JSON.stringify(event)
          }
        ]
      });

      console.log(`✓ Sent ${event.event_type} event to topic: ${topic}`);
    }

    console.log('\\n✓ All test messages sent successfully!');
    console.log('\\nCheck your n8n workflow for processing results.');

  } catch (error) {
    console.error('Error sending messages:', error);
  } finally {
    await producer.disconnect();
  }
}

// Run the test
sendTestMessages();
