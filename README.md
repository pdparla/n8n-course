# n8n Kafka AI Workflow

Automated workflows using n8n that process Kafka events with AI (Gemini) and take intelligent actions.

## Quick Start

### 1. Start Services

```bash
# Copy environment file
cp .env.example .env

# Start Kafka, Schema Registry, n8n, PostgreSQL, Kafka UI
npm run docker:up

# Wait ~30 seconds for services to be ready
```

Services:
- **n8n**: http://localhost:5678
- **Kafka UI**: http://localhost:8080
- **Schema Registry**: http://localhost:8081
- **Kafka Broker** (internal): `broker:29092`
- **Kafka Broker** (external): `localhost:9092`

### 2. Register Schemas

```bash
npm run schemas:register
```

This registers Avro schemas in the Schema Registry:
- `customer-feedback.avsc`
- `order-created.avsc`
- `system-log.avsc`

### 3. Import Workflow to n8n

1. Open n8n at http://localhost:5678
2. Click **"Import from File"**
3. Select `workflows/kafka-ai-workflow.json`
4. Configure credentials:
   - **Kafka**: Broker = `broker:29092` (for Docker n8n)
   - **Gemini API**: Create Generic Credential with `{"apiKey": "YOUR_KEY"}`
   - **PostgreSQL**: Already configured via environment variables

5. **Activate the workflow** (toggle switch in top right)

### 4. Send Test Messages

```bash
# Send JSON messages
npm run test:workflow

# Send Avro-encoded messages
npm run test:workflow:avro
```

Check execution results in n8n UI or Kafka UI.

## How It Works

```
Kafka Topic (ai-workflow-events)
    ↓
Kafka Trigger (n8n) - Listens to topic
    ↓
Extract Message Data - Parse JSON
    ↓
Gemini Analysis - AI analyzes event
    ↓
Process AI Response - Structure output
    ↓
Check Priority - Route based on analysis
    ↓
Actions:
  - High Priority → Telegram message
```

## Kafka Compose Configuration

The `docker/kafka-compose.yml` file sets up:

- **Kafka Broker** (Confluent Platform 8.0.0)
  - KRaft mode (no Zookeeper)
  - Ports: 9092 (external), 29092 (internal), 9101 (JMX)
  - Auto-creates topics

- **Schema Registry** (Confluent 8.0.0)
  - Port: 8081
  - Manages Avro schemas

- **n8n** (latest)
  - Port: 5678
  - PostgreSQL backend
  - Connected to Kafka network

- **PostgreSQL** (v15)
  - Port: 5432
  - Persistent storage for n8n

- **Kafka UI** (latest)
  - Port: 8080
  - Web interface for Kafka/Schema Registry

## Avro Schemas

Schemas define message structure and are stored in `schemas/`:


**Register schemas before sending Avro messages:**
```bash
npm run schemas:register
```

## Sending Messages to Topics

### JSON Messages (Simple)

```bash
npm run test:workflow
```

Sends 3 test events:
- Customer feedback
- Order created
- System log

### Avro Messages (Schema Registry)

```bash
# 1. Register schemas first
npm run schemas:register

# 2. Send Avro messages
npm run test:workflow:avro
```

**Custom messages:**

```javascript
// scripts/test-workflow.js or scripts/test-workflow-avro.js
const producer = kafka.producer();

await producer.send({
  topic: 'ai-workflow-events',
  messages: [
    {
      key: 'my-event-type',
      value: JSON.stringify({
        event_type: 'my-event-type',
        event_data: { /* your data */ },
        timestamp: new Date().toISOString()
      })
    }
  ]
});
```

## Available Commands

```bash
# Docker
npm run docker:up                    # Start all services
npm run docker:down                  # Stop all services

# Schemas
npm run schemas:register             # Register Avro schemas

# Testing
npm run test:workflow                # Send JSON messages
npm run test:workflow:avro           # Send Avro messages
```
