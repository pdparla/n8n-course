const fs = require('fs');
const path = require('path');
const axios = require('axios');

const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081';
const SCHEMAS_DIR = path.join(__dirname, '..', 'schemas');

// Schema files to register
const schemaFiles = [
  { file: 'event-value.avsc', subject: 'ai-workflow-events-value' },
  { file: 'customer-feedback.avsc', subject: 'ai-workflow-events-customer-feedback-value' },
  { file: 'order-created.avsc', subject: 'ai-workflow-events-order-created-value' },
  { file: 'system-log.avsc', subject: 'ai-workflow-events-system-log-value' }
];

async function registerSchema(subject, schemaPath) {
  try {
    const schemaContent = fs.readFileSync(schemaPath, 'utf8');
    const schema = JSON.parse(schemaContent);

    console.log(`\nRegistering schema: ${subject}`);
    console.log(`  File: ${path.basename(schemaPath)}`);

    const response = await axios.post(
      `${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions`,
      {
        schema: JSON.stringify(schema),
        schemaType: 'AVRO'
      },
      {
        headers: {
          'Content-Type': 'application/vnd.schemaregistry.v1+json'
        }
      }
    );

    console.log(`  ✓ Registered successfully! Schema ID: ${response.data.id}`);
    return response.data;

  } catch (error) {
    if (error.response) {
      console.error(`  ✗ Error: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    } else {
      console.error(`  ✗ Error: ${error.message}`);
    }
    throw error;
  }
}

async function checkSchemaRegistryHealth() {
  try {
    const response = await axios.get(`${SCHEMA_REGISTRY_URL}/`);
    console.log('✓ Schema Registry is healthy');
    return true;
  } catch (error) {
    console.error('✗ Schema Registry is not available at', SCHEMA_REGISTRY_URL);
    console.error('  Make sure Docker services are running: npm run docker:up');
    return false;
  }
}

async function listSchemas() {
  try {
    const response = await axios.get(`${SCHEMA_REGISTRY_URL}/subjects`);
    console.log('\nRegistered schemas:');
    if (response.data.length === 0) {
      console.log('  (none)');
    } else {
      response.data.forEach(subject => {
        console.log(`  - ${subject}`);
      });
    }
  } catch (error) {
    console.error('Error listing schemas:', error.message);
  }
}

async function main() {
  console.log('=== Schema Registry Setup ===\n');
  console.log(`Schema Registry URL: ${SCHEMA_REGISTRY_URL}`);

  // Check health
  const isHealthy = await checkSchemaRegistryHealth();
  if (!isHealthy) {
    process.exit(1);
  }

  // List existing schemas
  await listSchemas();

  console.log('\n=== Registering Schemas ===');

  // Register each schema
  let successCount = 0;
  for (const { file, subject } of schemaFiles) {
    const schemaPath = path.join(SCHEMAS_DIR, file);

    if (!fs.existsSync(schemaPath)) {
      console.error(`\n✗ Schema file not found: ${schemaPath}`);
      continue;
    }

    try {
      await registerSchema(subject, schemaPath);
      successCount++;
    } catch (error) {
      // Error already logged in registerSchema
    }
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Registered: ${successCount}/${schemaFiles.length} schemas`);

  // List schemas again
  await listSchemas();

  console.log('\n✓ Schema registration complete!');
  console.log('\nView schemas at:');
  console.log(`  - Schema Registry API: ${SCHEMA_REGISTRY_URL}`);
  console.log(`  - Kafka UI: http://localhost:8080`);
}

main().catch(error => {
  console.error('\nFatal error:', error.message);
  process.exit(1);
});
