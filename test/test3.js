const { DuckDBInstance } = require('@duckdb/node-api');
const path = require('path');

// ======================================================================
// КОНФИГУРАЦИЯ
// ======================================================================
const QUACK_URI = 'quack:localhost:9495';
const QUACK_TOKEN = 'Intra_secret';
const TABLE = 'records';
const QUERY_TIMEOUT_MS = 30000;
const NUM_REQUESTS = 10;

// Простые запросы для чередования
const QUERIES = [
  `SELECT COUNT(*) AS cnt FROM ${TABLE};`,
  `SELECT COUNT(*) AS cnt FROM ${TABLE} WHERE id = 8;`,
  `SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM ${TABLE};`,
  `SELECT id, COUNT(*) AS cnt FROM ${TABLE} GROUP BY id ORDER BY id LIMIT 5;`,
  `SELECT ts, id, val FROM ${TABLE} ORDER BY ts DESC LIMIT 10;`,
  `SELECT AVG(val) AS avg_val FROM ${TABLE} WHERE id IN (8, 9, 10);`,
];

// ======================================================================
// ПОДКЛЮЧЕНИЕ (без ATTACH!)
// ======================================================================
async function createConnection() {
  const extPath = path.join(__dirname, 'node_modules/@duckdb/extensions');
  const db = await DuckDBInstance.create(':memory:', {
    extension_directory: extPath,
  });
  const conn = await db.connect();

  // Убедимся, что quack загружен
  await conn.run(`FORCE INSTALL quack;`).catch(() => {});
  await conn.run(`LOAD quack;`);

  return { db, conn };
}

// ======================================================================
// ОБЁРТКА quack_query
// ======================================================================
// Согласно документации: quack_query(uri, query, token := '...', disable_ssl := false)
// Некоторые версии могут принимать token => '...'
function buildQuackQuery(innerSql) {
  // Экранируем одинарные кавычки в SQL для вложенной строки
  const escapedSql = innerSql.replace(/'/g, "''");
  return `
    SELECT * FROM quack_query(
      '${QUACK_URI}',
      '${escapedSql}',
      token := '${QUACK_TOKEN}',
      disable_ssl := true
    );
  `;
}

// ======================================================================
// ВЫПОЛНЕНИЕ / ТАЙМАУТ
// ======================================================================
async function runQuery(conn, sql) {
  const prepared = await conn.prepare(sql);
  const result = await prepared.run();
  return await result.getRowObjects();
}

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`TIMEOUT ${ms} мс: ${label}`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// ======================================================================
// ОСНОВНОЙ СЦЕНАРИЙ
// ======================================================================
async function main() {
  console.log('=== ТЕСТ: quack_query (без ATTACH), 10 запросов подряд ===\n');

  const { db, conn } = await createConnection();
  const results = [];

  try {
    for (let i = 0; i < NUM_REQUESTS; i++) {
      const innerSql = QUERIES[i % QUERIES.length];
      const outerSql = buildQuackQuery(innerSql);

      console.log(`--- Запрос #${i + 1} ---`);
      console.log(`  Inner SQL: ${innerSql}`);

      const t0 = Date.now();
      try {
        const rows = await withTimeout(
          runQuery(conn, outerSql),
          QUERY_TIMEOUT_MS,
          `#${i + 1}`
        );
        const ms = Date.now() - t0;
        console.log(`  OK: ${ms} мс, строк: ${rows.length}`);
        results.push({ n: i + 1, ms, status: 'OK' });
      } catch (err) {
        const ms = Date.now() - t0;
        console.error(`  FAIL: ${ms} мс, ошибка: ${err.message}`);
        results.push({ n: i + 1, ms, status: 'FAIL', error: err.message });
        if (String(err.message).includes('TIMEOUT')) {
          console.log(`\n[STOP] Таймаут на запросе #${i + 1}. Останавливаемся.`);
          break;
        }
      }
      console.log('');
    }
  } finally {
    try { conn.closeSync(); } catch (_) {}
    try { db.closeSync?.(); } catch (_) {}
  }

  // Итог
  console.log('\n' + '='.repeat(60));
  console.log('ИТОГ');
  console.log('='.repeat(60));
  console.table(results);

  const firstFail = results.find(r => r.status === 'FAIL');
  if (firstFail) {
    console.log(`\nПервый сбой на запросе #${firstFail.n}.`);
    if (firstFail.n === 7) {
      console.log('Гипотеза «7-й запрос зависает» ПОДТВЕРЖДЕНА и для quack_query.');
    } else {
      console.log(`Гипотеза НЕ подтверждена для quack_query: сбой на #${firstFail.n}, а не на #7.`);
    }
  } else {
    console.log(`\nВсе ${results.length} запросов выполнились через quack_query.`);
    console.log('Гипотеза «7-й запрос зависает» НЕ подтверждена для quack_query.');
    console.log('Это означает, что stateless-режим обходит проблему ATTACH.');
  }
}

main().catch((err) => {
  console.error('FATAL:', err);
  process.exit(1);
});