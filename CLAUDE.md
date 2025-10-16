# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an n8n course repository that teaches how to build workflows integrating Kafka event streams with AI agents (Gemini/OpenAI/Anthropic). The core workflow pattern is: Kafka Trigger → Extract Data → AI Analysis → Conditional Processing → Actions (database storage, alerts).

**Deployment**: Fully containerized with Docker Compose (n8n, Kafka with KRaft, PostgreSQL, Kafka UI) or hybrid (local n8n + Docker Kafka).

## Development Commands

### Docker Operations (Recommended)
```bash
npm run docker:up             # Start all services (n8n, Kafka, PostgreSQL, etc.)
npm run docker:down           # Stop all services
npm run docker:logs           # View logs from all services
npm run docker:logs:n8n       # View n8n logs only
npm run docker:restart        # Restart n8n container
```

Services accessible at:
- **n8n UI**: `http://localhost:5678`
- **Kafka UI**: `http://localhost:8080`
- **Schema Registry**: `http://localhost:8081`
- **PostgreSQL**: `localhost:5432`

### Local n8n Operations (Alternative)
```bash
npm run n8n                    # Start n8n locally (standard mode)
npm run n8n:dev               # Start n8n locally with webhook tunnel
```
- Requires n8n installed globally: `npm install -g n8n`
- Still needs Docker for Kafka: `npm run docker:up`

### Testing & Schema Management
```bash
npm run schemas:register      # Register Avro schemas in Schema Registry
npm run test:workflow         # Send JSON messages to Kafka topic
npm run test:workflow:avro    # Send Avro-encoded messages to Kafka topic
```

## Architecture

### Workflow Structure
The main workflow ([workflows/kafka-ai-workflow.json](workflows/kafka-ai-workflow.json)) follows this pattern:

1. **Kafka Trigger** - Listens to `ai-workflow-events` topic
2. **Extract Message Data** - Parses event JSON structure
3. **Gemini Analysis** - Google Gemini AI analyzes the event (uses HTTP Request node)
4. **Process Response** - Code node structures the AI output
5. **Check Priority** - IF node routes based on priority
6. **Actions**:
   - High priority: Webhook alert
   - Normal priority: Database storage

### Message Format
All Kafka messages follow this structure:
```json
{
  "event_type": "string",
  "event_data": { ... },
  "timestamp": "ISO8601"
}
```

### Docker Infrastructure
Complete stack in [docker/kafka-compose.yml](docker/kafka-compose.yml):
- **Kafka Broker** (Confluent Platform 8.0.0): Message broker with KRaft consensus - no Zookeeper needed!
  - Port 9092 (external), 29092 (internal), 9101 (JMX)
- **Schema Registry** (Confluent 8.0.0): Manages Avro schemas
  - Port 8081
- **n8n**: Workflow automation platform with PostgreSQL backend
  - Port 5678
- **PostgreSQL** (v15): n8n database storage
  - Port 5432
- **Kafka UI**: Web interface for Kafka/Schema Registry management
  - Port 8080
- **Network**: `n8n-kafka-network` (bridge)
- **Volumes**: `n8n_data`, `postgres_data` for persistence (Kafka uses ephemeral storage)

## Key Configuration

### Environment Variables
Copy `.env.example` to `.env`:
```bash
N8N_HOST=localhost
POSTGRES_USER=n8n
POSTGRES_PASSWORD=n8n_password
POSTGRES_DB=n8n
```

### Kafka Settings
**For Docker n8n** (inside Docker network):
- Broker: `broker:29092` (internal network address)
- Topic: `ai-workflow-events`
- Consumer Group: `n8n-consumer-group`
- Auto-create topics: Enabled

**For local n8n** (outside Docker network):
- Broker: `localhost:9092` (external port)
- Topic: `ai-workflow-events`
- Consumer Group: `n8n-consumer-group`

### AI Providers
Configure credentials in n8n:
- **Gemini** (default): Free tier available, create Generic Credential with `{"apiKey": "YOUR_KEY"}`
  - Get API key: https://makersuite.google.com/app/apikey
  - Model: `gemini-1.5-flash` (fast and free)
- **OpenAI**: Requires API key, uses GPT-4
- **Anthropic**: Alternative provider, requires API key

### n8n Expressions
Use n8n's expression syntax in workflow nodes:
- `{{ $json.field_name }}` - Access current node JSON
- `{{ $('Node Name').first().json.field }}` - Access specific node output
- `{{ JSON.stringify($json.object) }}` - Serialize objects

## Documentation

All documentation is in [README.md](README.md):
- How to run kafka-compose.yml
- How to register Avro schemas
- How to send messages to topics (JSON and Avro)
- How to import workflow to n8n
- Troubleshooting guide

## Working with Workflows

### Importing Workflows
1. Open n8n at `localhost:5678`
2. Click "Import from File"
3. Select workflow JSON from `workflows/`
4. Configure credentials (Kafka, OpenAI/Anthropic)

### Modifying Workflows
- n8n workflows are JSON files
- Edit directly or through n8n UI
- Export updated workflows back to `workflows/` directory

### Testing
1. Start all services: `npm run docker:up`
2. Wait for services to be ready (~30 seconds)
3. Register schemas: `npm run schemas:register`
4. Open n8n at `localhost:5678` and import workflow
5. Configure Kafka credentials (broker: `broker:29092` for Docker n8n)
6. Configure Gemini API credentials
7. Activate workflow in n8n UI
8. Send test messages: `npm run test:workflow` or `npm run test:workflow:avro`
9. Check execution in n8n UI or Kafka UI

## Common Tasks

### Adding New Event Types
1. Update `scripts/test-workflow.js` with new event structure
2. Modify AI prompt in workflow to handle new type
3. Test with `npm run test:workflow`

### Changing AI Model
In workflow JSON, update OpenAI node:
```json
{
  "parameters": {
    "model": "gpt-3.5-turbo"  // or "gpt-4", "gpt-4-turbo"
  }
}
```

### Using Different AI Providers
The workflow currently uses Gemini via HTTP Request node. To switch:

**Gemini (current):**
- URL: `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent`
- Free tier: 15 requests/min, 1M tokens/min

**OpenAI:**
- Replace HTTP Request node with OpenAI node
- Model: `gpt-4` or `gpt-3.5-turbo`

**Anthropic:**
- Use HTTP Request node
- Endpoint: `https://api.anthropic.com/v1/messages`
- Model: `claude-3-5-sonnet-20241022`

## Troubleshooting

### All Services
```bash
docker ps                     # Check all running containers
npm run docker:logs          # View all logs
npm run docker:logs:n8n      # View n8n logs only
```

### Kafka Connection Issues
```bash
docker ps | grep broker      # Check if Kafka running
docker logs broker           # Check Kafka logs
docker restart broker        # Restart Kafka
```

**Common issue**: Wrong broker address
- Docker n8n: Use `broker:29092` (internal network)
- Local n8n: Use `localhost:9092` (external port)

### Workflow Not Triggering
- Verify Kafka credentials in n8n (broker address is critical!)
- Check consumer group hasn't committed offsets past test messages
- Use new consumer group ID or reset offsets
- Verify topic name matches: `ai-workflow-events`
- Check Kafka UI at `localhost:8080` for messages in topic

### PostgreSQL Connection Issues
```bash
docker ps | grep postgres                    # Check if running
docker logs n8n-postgres                     # View logs
docker exec -it n8n-postgres psql -U n8n    # Connect to DB
```

### n8n Execution Errors
- Check workflow execution logs in n8n UI
- Verify all credentials are configured
- Test nodes individually using "Execute Node" button

### AI API Errors
- Verify API keys are valid
- Check rate limits on provider dashboard
- Review prompt length (token limits)

## File Organization

```
n8n-course/
├── workflows/
│   └── kafka-ai-workflow.json      # Main n8n workflow (uses Gemini)
├── docker/
│   └── kafka-compose.yml           # Kafka + Schema Registry + n8n stack
├── schemas/                        # Avro schema definitions
│   ├── event-value.avsc
│   ├── customer-feedback.avsc
│   ├── order-created.avsc
│   └── system-log.avsc
├── scripts/
│   ├── test-workflow.js            # Send JSON messages
│   ├── test-workflow-avro.js       # Send Avro messages
│   └── register-schemas.js         # Register schemas in Registry
├── package.json                    # npm scripts and dependencies
├── .env.example                    # Environment template
├── CLAUDE.md                       # This file (AI assistant guidance)
└── README.md                       # Main documentation
```

## Best Practices

### Workflow Development
- Test nodes individually before running full workflow
- Use meaningful node names
- Add error handling nodes
- Monitor execution history in n8n

### Kafka Operations
- Use descriptive consumer group IDs
- Monitor lag in Kafka UI
- Handle message deserialization errors
- Implement idempotency for duplicate messages

### AI Integration
- Keep prompts concise to reduce costs
- Use structured output formats (JSON)
- Set appropriate max_tokens limits
- Implement fallback logic for API failures

### Production Readiness
- Implement dead letter queues for failed messages
- Add circuit breakers for cascading failure prevention
- Monitor AI API usage and costs
- Set up alerting for workflow failures
