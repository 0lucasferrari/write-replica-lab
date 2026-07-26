const pgPool = require('./db/postgres');
const esClient = require('./db/elasticsearch');

async function processOutbox() {
  const client = await pgPool.connect();

  try {
    await client.query('BEGIN');

    // 1️⃣ Busca eventos pendentes, travando as linhas (FOR UPDATE SKIP LOCKED)
    const { rows: events } = await client.query(
      `SELECT * FROM outbox
       WHERE processed = FALSE
       ORDER BY created_at ASC
       LIMIT 10
       FOR UPDATE SKIP LOCKED`
    );

    for (const event of events) {
      const payload = event.payload;

      if (event.event_type === 'created' || event.event_type === 'updated') {
        await esClient.index({
          index: 'products',
          id: event.aggregate_id.toString(),
          document: {
            id: payload.id,
            name: payload.name,
            description: payload.description,
            price: parseFloat(payload.price),
            created_at: payload.created_at,
            updated_at: payload.updated_at,
          },
        });
      }

      // 2️⃣ Marca como processado
      await client.query(
        `UPDATE outbox SET processed = TRUE WHERE id = $1`,
        [event.id]
      );

      console.log(`✅ Evento outbox #${event.id} processado (produto id=${event.aggregate_id})`);
    }

    await client.query('COMMIT');

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('❌ Erro ao processar outbox:', err.message);

  } finally {
    client.release();
  }
}

module.exports = processOutbox;