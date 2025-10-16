const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const avro = require('avro-js');
const fs = require('fs');
const path = require('path');

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';
const TOPIC = 'ai-workflow-events';

// Suppress KafkaJS partitioner warning
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

// Initialize Kafka and Schema Registry
const kafka = new Kafka({
  clientId: 'n8n-test-producer-avro',
  brokers: [KAFKA_BROKER]
});

const registry = new SchemaRegistry({
  host: SCHEMA_REGISTRY_URL
});

const producer = kafka.producer();

// Load schemas
const schemasDir = path.join(__dirname, '..', 'schemas');

// Sample events with their schema files
const sampleEvents = [
  {
    schemaFile: 'customer-feedback.avsc',
    subject: 'ai-workflow-events-customer-feedback-value',
    data: {
      event_type: 'customer_feedback',
      event_data: {
        customer_id: 'CUST-12345',
        feedback: 'The product quality is excellent, but delivery was delayed by 3 days.',
        rating: 4,
        product_id: 'PROD-789',
        order_id: 'ORD-45678'
      },
      timestamp: new Date().toISOString()
    }
  },
  {
    schemaFile: 'order-created.avsc',
    subject: 'ai-workflow-events-order-created-value',
    data: {
      event_type: 'order_created',
      event_data: {
        order_id: 'ORD-98765',
        customer_id: 'CUST-54321',
        items: [
          { product_id: 'PROD-001', quantity: 2, price: 29.99 },
          { product_id: 'PROD-042', quantity: 1, price: 149.99 }
        ],
        total_amount: 209.97,
        currency: 'USD',
        status: 'PENDING'
      },
      timestamp: new Date().toISOString()
    }
  },
  {
    schemaFile: 'system-log.avsc',
    subject: 'ai-workflow-events-system-log-value',
    data: {
      event_type: 'system_log',
      event_data: {
        level: 'ERROR',
        service: 'payment-gateway',
        message: 'Connection timeout to payment provider after 3 retries',
        error_code: 'PAYMENT_TIMEOUT',
        stack_trace: null,
        host: 'payment-service-01'
      },
      timestamp: new Date().toISOString()
    }
  }
];

async function encodeWithSchemaRegistry(subject, schema, data) {
  try {
    // Get or register the schema and get its ID
    const { id } = await registry.register(schema, { subject });

    // Encode the message with the schema ID
    const encodedMessage = await registry.encode(id, data);

    return encodedMessage;
  } catch (error) {
    console.error(`Error encoding message for ${subject}:`, error.message);
    console.error('Full error:', error);
    throw error;
  }
}

async function sendAvroMessages() {
  try {
    await producer.connect();
    console.log('✓ Connected to Kafka');

    for (const event of sampleEvents) {
      try {
        // Load the schema
        const schemaPath = path.join(schemasDir, event.schemaFile);
        const schemaContent = fs.readFileSync(schemaPath, 'utf8');
        const schema = JSON.parse(schemaContent);

        // Encode message using Schema Registry
        const encodedValue = await encodeWithSchemaRegistry(event.subject, schema, event.data);

        // Send to Kafka
        await producer.send({
          topic: TOPIC,
          messages: [
            {
              key: event.data.event_type,
              value: encodedValue
            }
          ]
        });

        console.log(`✓ Sent ${event.data.event_type} event (Avro-encoded) to topic: ${TOPIC}`);
        console.log(`  Schema: ${event.subject}`);

      } catch (error) {
        console.error(`✗ Failed to send ${event.data.event_type}:`, error.message);
      }
    }

    console.log('\\n✓ All Avro test messages sent successfully!');
    console.log('\\nCheck your n8n workflow for processing results.');
    console.log('View messages in Kafka UI: http://localhost:8080');

  } catch (error) {
    console.error('Error:', error.message);
    console.error('\\nMake sure:');
    console.error('  1. Kafka is running: docker ps | grep kafka');
    console.error('  2. Schema Registry is running: docker ps | grep schema-registry');
    console.error('  3. Schemas are registered: npm run schemas:register');
  } finally {
    await producer.disconnect();
  }
}

// Check if schemas are registered first
async function checkSchemas() {
  console.log('Checking Schema Registry...');
  try {
    const response = await fetch(`${SCHEMA_REGISTRY_URL}/subjects`);
    const subjects = await response.json();

    if (subjects.length === 0) {
      console.warn('⚠ No schemas registered in Schema Registry');
      console.warn('  Run: npm run schemas:register');
      return false;
    }

    console.log(`✓ Found ${subjects.length} registered schemas`);
    return true;
  } catch (error) {
    console.error('✗ Schema Registry not available:', error.message);
    console.error('  Make sure Docker services are running: npm run docker:up');
    return false;
  }
}

// Main execution
async function main() {
  console.log('=== n8n Kafka Avro Test Producer ===\\n');

  const schemasOk = await checkSchemas();
  if (!schemasOk) {
    console.log('\\nPlease fix the issues above before continuing.');
    process.exit(1);
  }

  console.log('');
  await sendAvroMessages();
}

main();
