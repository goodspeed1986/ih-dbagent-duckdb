const { DuckDBInstance } = require('@duckdb/node-api');
const path = require('path');

// ======================================================================
// КОНФИГУРАЦИЯ
// ======================================================================
const QUACK_URL = 'quack:localhost:9495';
const QUACK_TOKEN = 'Intra_secret';
const TABLE = 'records';
const TMP_DIR = '/tmp';
const QUERY_TIMEOUT_MS = 30000;       // таймаут на EXPLAIN ANALYZE
const PARQUET_TIMEOUT_MS = 120000;    // COPY в parquet может быть дольше

// ======================================================================
// ПОДКЛЮЧЕНИЕ
// ======================================================================
async function createConnection() {
  const extPath = path.join(__dirname, 'node_modules/@duckdb/extensions');

  const db = await DuckDBInstance.create(':memory:', {
    extension_directory: extPath,
  });

  return await connectToDb(db);
}

async function connectToDb(db) {
  const conn = await db.connect();

  await conn.run(`LOAD quack;`);

  await conn.run(`
    CREATE OR REPLACE SECRET (
      TYPE quack,
      TOKEN '${QUACK_TOKEN}',
      SCOPE '${QUACK_URL}'
    );
  `);

  await conn.run(`
    ATTACH '${QUACK_URL}' AS remote_db (TYPE quack);
  `);

  return conn;
}

// ======================================================================
// ПРЕФИКС remote_db. ДЛЯ FROM/JOIN
// ======================================================================
function addRemotePrefix(sql) {
  if (!sql || typeof sql !== 'string') return sql;

  return sql.replace(
    /\b(FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?=\s|$|\(|\,|;|\s+AS|\s+ON|\s+WHERE)/gi,
    (match, keyword, table) => {
      if (
        table.toLowerCase() === 'remote_db' ||
        /\.|(select|values|\(|\)|,)/i.test(table)
      ) {
        return match;
      }
      return `${keyword} remote_db.${table}`;
    }
  );
}

// ======================================================================
// ВЫПОЛНЕНИЕ ЗАПРОСА С БИНДИНГАМИ
// ======================================================================
async function runQuery(conn, sql, values = []) {
  const prepared = await conn.prepare(sql);
  for (let i = 0; i < values.length; i++) {
    await prepared.bind(i + 1, values[i]);
  }
  const result = await prepared.run();
  return await result.getRowObjects();
}

// ======================================================================
// ТАЙМАУТ
// ======================================================================
function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`TIMEOUT ${ms} мс: ${label}`)),
      ms
    );
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// ======================================================================
// ПАРСИНГ МЕТРИК ИЗ ПЛАНА (TABLE_SCAN вместо REMOTE_SCAN)
// ======================================================================
function parsePlanMetrics(planText) {
  const metrics = {
    scanRows: null,
    scanTimeMs: null,
    aggregateRows: null,
    aggregateTimeMs: null,
    sortRows: null,
    sortTimeMs: null,
    filterRows: null,
    filterTimeMs: null,
    totalTimeMs: null,
    rawPlan: planText,
  };

  const lines = planText.split('\n');

  for (const line of lines) {
    const lower = line.toLowerCase();

    const timingMatch = line.match(/(?:timing[=:]\s*)?([\d.]+)\s*s\b/i);
    const rowsMatch =
      line.match(/([\d,]+)\s+rows?/i) ||
      line.match(/rows[=:]\s*(\d+)/i);

    // TABLE_SCAN — это удалённый скан через quack
    if (lower.includes('table_scan') || lower.includes('table scan')) {
      if (rowsMatch) metrics.scanRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) metrics.scanTimeMs = parseFloat(timingMatch[1]) * 1000;
    }

    if (
      lower.includes('hash_group_by') ||
      lower.includes('ungrouped_aggregate')
    ) {
      if (rowsMatch) metrics.aggregateRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) metrics.aggregateTimeMs = parseFloat(timingMatch[1]) * 1000;
    }

    if (lower.includes('order_by') || lower.includes('top_n')) {
      if (rowsMatch) metrics.sortRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) metrics.sortTimeMs = parseFloat(timingMatch[1]) * 1000;
    }

    if (lower.includes('filter')) {
      if (rowsMatch) metrics.filterRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) metrics.filterTimeMs = parseFloat(timingMatch[1]) * 1000;
    }

    const totalMatch = line.match(/Total Time:\s*([\d.]+)\s*s/i);
    if (totalMatch) metrics.totalTimeMs = parseFloat(totalMatch[1]) * 1000;
  }

  return metrics;
}

// ======================================================================
// МЕНЕДЖЕР СОЕДИНЕНИЯ: пересоздаёт БАЗУ и conn после таймаута
// ======================================================================
class ConnManager {
  constructor(db, conn) {
    this.db = db;
    this.conn = conn;
  }

  async reset() {
    try { this.conn.closeSync(); } catch (_) { /* ignore */ }
    try { this.db.closeSync?.(); } catch (_) { /* ignore */ }

    const extPath = path.join(__dirname, 'node_modules/@duckdb/extensions');
    this.db = await DuckDBInstance.create(':memory:', {
      extension_directory: extPath,
    });
    this.conn = await connectToDb(this.db);
    console.warn('[ConnManager] База и соединение пересозданы.');
  }
}
// ======================================================================
// АГРЕГ-ТЕСТ: EXPLAIN ANALYZE + метрики + таймаут + reconnect
// ======================================================================
async function aggregateTest(cm, label, sql) {
  console.log(`\n${'='.repeat(74)}`);
  console.log(`ТЕСТ: ${label}`);
  console.log(`SQL: ${sql}`);
  console.log('='.repeat(74));

  const processed = addRemotePrefix(sql);
  const explainSql = `EXPLAIN ANALYZE ${processed}`;

  let planText = '';
  try {
    const rows = await withTimeout(
      runQuery(cm.conn, explainSql),
      QUERY_TIMEOUT_MS,
      label
    );
    for (const row of rows) {
      for (const [, val] of Object.entries(row)) {
        if (typeof val === 'string') planText += val + '\n';
      }
    }
    console.log('--- ПЛАН ---');
    console.log(planText);
  } catch (err) {
    console.error(`ОШИБКА в тесте "${label}":`, err.message);
    if (String(err.message).includes('TIMEOUT')) {
      await cm.reset();
    }
    return null;
  }

  const m = parsePlanMetrics(planText);

  console.log('--- МЕТРИКИ ---');
  console.log(`  TABLE_SCAN rows:    ${m.scanRows ?? 'n/a'}`);
  console.log(`  TABLE_SCAN time:    ${m.scanTimeMs ?? 'n/a'} ms`);
  console.log(`  FILTER rows:        ${m.filterRows ?? 'n/a'}`);
  console.log(`  FILTER time:        ${m.filterTimeMs ?? 'n/a'} ms`);
  console.log(`  AGGREGATE rows:     ${m.aggregateRows ?? 'n/a'}`);
  console.log(`  AGGREGATE time:     ${m.aggregateTimeMs ?? 'n/a'} ms`);
  console.log(`  SORT rows:          ${m.sortRows ?? 'n/a'}`);
  console.log(`  SORT time:          ${m.sortTimeMs ?? 'n/a'} ms`);
  console.log(`  TOTAL time:         ${m.totalTimeMs ?? 'n/a'} ms`);

  if (m.scanTimeMs != null && m.totalTimeMs != null && m.totalTimeMs > 0) {
    const pct = ((m.scanTimeMs / m.totalTimeMs) * 100).toFixed(1);
    console.log(`  Доля TABLE_SCAN:    ${pct}%`);
  }

  return m;
}

// ======================================================================
// PARQUET-ТЕСТ: отдельный, с reconnect при таймауте
// ======================================================================
async function aggregateToParquetTest(cm, label, sql) {
  console.log(`\n${'='.repeat(74)}`);
  console.log(`PARQUET-ТЕСТ: ${label}`);
  console.log(`SQL: ${sql}`);
  console.log('='.repeat(74));

  const parquetPath = `${TMP_DIR}/agg_${Date.now()}_${Math.random().toString(36).slice(2)}.parquet`;
  const copySql = addRemotePrefix(`
    COPY (
      ${sql}
    ) TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
  `);

  try {
    const t0 = Date.now();
    await withTimeout(runQuery(cm.conn, copySql), PARQUET_TIMEOUT_MS, label);
    const ms = Date.now() - t0;
    console.log(`  COPY в Parquet завершён за ${ms} мс`);
    console.log(`  Файл: ${parquetPath}`);

    const check = await withTimeout(
      runQuery(cm.conn, `SELECT COUNT(*) AS cnt FROM read_parquet('${parquetPath}');`),
      QUERY_TIMEOUT_MS,
      'read_parquet check'
    );
    console.log(`  Строк в Parquet: ${check[0].cnt}`);
    return ms;
  } catch (err) {
    console.error(`ОШИБКА в Parquet-тесте "${label}":`, err.message);
    if (String(err.message).includes('TIMEOUT')) {
      await cm.reset();
    }
    return null;
  }
}

// ======================================================================
// ОСНОВНОЙ СЦЕНАРИЙ
// ======================================================================
async function main() {
  const extPath = path.join(__dirname, 'node_modules/@duckdb/extensions');
  const db = await DuckDBInstance.create(':memory:', {
    extension_directory: extPath,
  });
  const conn = await connectToDb(db);
  const cm = new ConnManager(db, conn);

  try {
    // ------------------------------------------------------------------
    // 0. Схема
    // ------------------------------------------------------------------
    console.log(`\n=== СХЕМА ТАБЛИЦЫ ${TABLE} ===`);
    try {
      const schema = await withTimeout(
        runQuery(cm.conn, `DESCRIBE remote_db.${TABLE};`),
        QUERY_TIMEOUT_MS,
        'DESCRIBE'
      );
      console.table(schema);
    } catch (err) {
      console.error(`Не удалось получить схему:`, err.message);
      return;
    }

    // ------------------------------------------------------------------
    // Базовые цифры
    // ------------------------------------------------------------------
    console.log(`\n=== БАЗОВЫЕ ЦИФРЫ ===`);
    try {
      const totalRows = await withTimeout(
        runQuery(cm.conn, `SELECT COUNT(*) AS cnt FROM remote_db.${TABLE};`),
        QUERY_TIMEOUT_MS,
        'COUNT(*) total'
      );
      console.log(`Всего строк в ${TABLE}: ${totalRows[0].cnt}`);
    } catch (err) {
      console.error('ОШИБКА COUNT(*):', err.message);
    }

    // ------------------------------------------------------------------
    // Список id для фильтра
    // ------------------------------------------------------------------
    const IDS = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18];
    const idList = IDS.join(', ');

    // ==================================================================
    // БЛОК 1: Базовые агрегаты без GROUP BY
    // ==================================================================
    await aggregateTest(
      cm,
      '1a: COUNT(*) без фильтра',
      `SELECT COUNT(*) AS cnt FROM ${TABLE};`
    );

    await aggregateTest(
      cm,
      '1b: COUNT(*) + SUM(val) + AVG(val)',
      `SELECT COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
       FROM ${TABLE};`
    );

    await aggregateTest(
      cm,
      '1c: COUNT(*) WHERE id IN (8..18)',
      `SELECT COUNT(*) AS cnt
       FROM ${TABLE}
       WHERE id IN (${idList});`
    );

       // ==================================================================
    // БЛОК 2: GROUP BY id (с фильтром, чтобы не съесть память)
    // ==================================================================
    await aggregateTest(
      cm,
      '2a: GROUP BY id WHERE id IN (8..18) + COUNT/SUM/AVG',
      `SELECT id, COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
       FROM ${TABLE}
       WHERE id IN (${idList})
       GROUP BY id
       ORDER BY id;`
    );

    await aggregateTest(
      cm,
      '2b: GROUP BY id WHERE id BETWEEN 8 AND 18',
      `SELECT id, COUNT(*) AS cnt, SUM(val) AS total
       FROM ${TABLE}
       WHERE id BETWEEN 8 AND 18
       GROUP BY id
       ORDER BY id;`
    );

    // 2c: HAVING на отфильтрованных данных (а не на всей таблице)
    await aggregateTest(
      cm,
      '2c: GROUP BY id WHERE id IN (8..18) HAVING COUNT(*) > 100',
      `SELECT id, COUNT(*) AS cnt
       FROM ${TABLE}
       WHERE id IN (${idList})
       GROUP BY id
       HAVING COUNT(*) > 100
       ORDER BY id;`
    );

    // ==================================================================
    // БЛОК 3: Агрегация с ORDER BY + LIMIT (проблемный паттерн на id)
    // ==================================================================
    await aggregateTest(
      cm,
      '3a: GROUP BY id ORDER BY cnt DESC LIMIT 5',
      `SELECT id, COUNT(*) AS cnt
       FROM ${TABLE}
       GROUP BY id
       ORDER BY cnt DESC
       LIMIT 5;`
    );

    await aggregateTest(
      cm,
      '3b: GROUP BY id ORDER BY id LIMIT 5',
      `SELECT id, COUNT(*) AS cnt
       FROM ${TABLE}
       GROUP BY id
       ORDER BY id
       LIMIT 5;`
    );

    await aggregateTest(
      cm,
      '3c: GROUP BY id в подзапросе, ORDER BY+LIMIT снаружи',
      `SELECT id, cnt
       FROM (
         SELECT id, COUNT(*) AS cnt
         FROM ${TABLE}
         GROUP BY id
       ) sub
       ORDER BY cnt DESC
       LIMIT 5;`
    );

    // ==================================================================
    // БЛОК 4: Фильтр по id — разные формы
    // ==================================================================
    await aggregateTest(
      cm,
      '4a: IN (8..18)',
      `SELECT id, COUNT(*) AS cnt
       FROM ${TABLE}
       WHERE id IN (${idList})
       GROUP BY id
       ORDER BY id;`
    );

    await aggregateTest(
      cm,
      '4b: BETWEEN 8 AND 18',
      `SELECT id, COUNT(*) AS cnt
       FROM ${TABLE}
       WHERE id BETWEEN 8 AND 18
       GROUP BY id
       ORDER BY id;`
    );

    const orChain = IDS.map((id) => `id = ${id}`).join(' OR ');
    await aggregateTest(
      cm,
      '4c: OR-цепочка (id = 8 OR ... OR id = 18)',
      `SELECT id, COUNT(*) AS cnt
       FROM ${TABLE}
       WHERE ${orChain}
       GROUP BY id
       ORDER BY id;`
    );

    // ==================================================================
    // БЛОК 5: Параметризованный IN через биндинги
    // ==================================================================
    console.log(`\n${'='.repeat(74)}`);
    console.log('5: Параметризованный IN через биндинги');
    console.log('='.repeat(74));
    try {
      const placeholders = IDS.map((_, i) => `$${i + 1}`).join(', ');
      const sql = addRemotePrefix(
        `SELECT id, COUNT(*) AS cnt, SUM(val) AS total
         FROM ${TABLE}
         WHERE id IN (${placeholders})
         GROUP BY id
         ORDER BY id;`
      );
      const t0 = Date.now();
      const rows = await withTimeout(
        runQuery(cm.conn, sql, IDS),
        QUERY_TIMEOUT_MS,
        'параметризованный IN'
      );
      const ms = Date.now() - t0;
      console.log(`  Выполнено за ${ms} мс, строк: ${rows.length}`);
      console.table(rows);
    } catch (err) {
      console.error('ОШИБКА 5:', err.message);
      if (String(err.message).includes('TIMEOUT')) {
        await cm.reset();
      }
    }

    // ==================================================================
    // БЛОК 6: Двойная агрегация
    // ==================================================================
    await aggregateTest(
      cm,
      '6: AVG по COUNT (GROUP BY id)',
      `SELECT AVG(cnt) AS avg_cnt, MAX(cnt) AS max_cnt
       FROM (
         SELECT id, COUNT(*) AS cnt
         FROM ${TABLE}
         GROUP BY id
       ) sub;`
    );

    // ==================================================================
    // БЛОК 7: Parquet — эталон чистого времени
    // ==================================================================
    const parquetMs = await aggregateToParquetTest(
      cm,
      '7: GROUP BY id → Parquet',
      `SELECT id, COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
       FROM ${TABLE}
       GROUP BY id
       ORDER BY id`
    );

    // ==================================================================
    // ИТОГ
    // ==================================================================
    console.log(`\n${'='.repeat(74)}`);
    console.log('ИТОГ');
    console.log('='.repeat(74));
    console.log(`Время агрегации → Parquet: ${parquetMs ?? 'n/a'} мс`);
    console.log('\nИнтерпретация:');
    console.log('  - TABLE_SCAN rows ≈ полному числу строк во ВСЕХ тестах:');
    console.log('    quack не pushdown-ит НИЧЕГО, передаёт всю таблицу по сети.');
    console.log('  - FILTER rows = TABLE_SCAN rows:');
    console.log('    фильтр по id применяется ЛОКАЛЬНО после передачи.');
    console.log('  - Parquet-тест (7):');
    console.log('    если время ≈ тесту 2a — узкое место в сканировании quack,');
    console.log('    если заметно меньше — узкое место в передаче строк в Node.');
  } finally {
    try { cm.conn.closeSync(); } catch (_) { /* ignore */ }
    try { db.closeSync?.(); } catch (_) { /* ignore */ }
  }
}

main().catch((err) => {
  console.error('FATAL:', err);
  process.exit(1);
});