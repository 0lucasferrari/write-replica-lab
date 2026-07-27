require('dotenv').config();
const { Kafka } = require('kafkajs');
const esClient = require('./db/elasticsearch');

const kafka = new Kafka({
  clientId: 'write-replica-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'outbox-to-elasticsearch' });

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'wr.public.outbox', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return; // mensagens de tombstone (delete) têm value null

      const event = JSON.parse(message.value.toString());
      const { op, after } = event.payload;

      // Só processa criação/atualização, ignora eventos gerados pelo próprio UPDATE de "processed"
      if (op !== 'c' && op !== 'r') {
        console.log(`⏭️  Ignorando evento op="${op}" (provavelmente UPDATE de campo processed)`);
        return;
      }

      const product = JSON.parse(after.payload); // payload é uma string JSON dentro do JSON

      await esClient.index({
        index: 'products',
        id: product.id.toString(),
        document: {
          id: product.id,
          name: product.name,
          description: product.description,
          price: parseFloat(product.price),
          created_at: product.created_at,
          updated_at: product.updated_at,
        },
      });

      console.log(`✅ [Kafka] Produto id=${product.id} indexado no Elasticsearch via CDC`);
    },
  });
}

startConsumer().catch((err) => {
  console.error('❌ Erro no consumer Kafka:', err);
  process.exit(1);
});