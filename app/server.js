require('dotenv').config();
require('./kafka-consumer');
const express = require('express');
const pgPool = require('./db/postgres');
const esClient = require('./db/elasticsearch');

const app = express();
app.use(express.json());

app.post('/products', async (req, res) => {
  const { name, description, price } = req.body;
  const client = await pgPool.connect();

  try {
    await client.query('BEGIN');

    // 1️⃣ Insere o produto
    const pgResult = await client.query(
      `INSERT INTO products (name, description, price)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [name, description, price]
    );
    const product = pgResult.rows[0];

    // 2️⃣ Insere o evento na outbox — MESMA transação
    await client.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ($1, $2, $3, $4)`,
      ['product', product.id, 'created', JSON.stringify(product)]
    );

    await client.query('COMMIT');
    res.status(201).json(product);

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Erro na transação:', err.message);
    res.status(500).json({ error: err.message });

  } finally {
    client.release();
  }
});


// const processOutbox = require('./outbox-poller');

// setInterval(() => {
//   processOutbox();
// }, 5000);

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server rodando em http://localhost:${PORT}`);
});