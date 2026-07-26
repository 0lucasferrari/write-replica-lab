require('dotenv').config();
const pgPool = require('./db/postgres');
const esClient = require('./db/elasticsearch');

async function testPostgres() {
  try {
    const result = await pgPool.query('SELECT NOW()');
    console.log('✅ Postgres conectado. Hora do servidor:', result.rows[0].now);
  } catch (err) {
    console.error('❌ Erro ao conectar no Postgres:', err.message);
  }
}

async function testElasticsearch() {
  try {
    const info = await esClient.info();
    console.log('✅ Elasticsearch conectado. Cluster:', info.cluster_name);
  } catch (err) {
    console.error('❌ Erro ao conectar no Elasticsearch:', err.message);
  }
}

async function main() {
  await testPostgres();
  await testElasticsearch();
  process.exit(0);
}

main();