const { DuckDBInstance } = require('@duckdb/node-api');
const path = require('path');

// ======================================================================
// КОНФИГУРАЦИЯ
// ======================================================================
const QUACK_URL = 'quack:localhost:9495';
const QUACK_TOKEN = 'Intra_secret';
const TABLE = 'records';
const TMP_DIR = '/tmp';
const QUERY_TIMEOUT_MS = 60000;
const PARQUET_TIMEOUT_MS = 240000;

const IDS = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18];
const idList = IDS.join(', ');

const BUCKET_1MIN = 60;
const BUCKET_10MIN = 600;
const BUCKET_1HOUR = 3600;
const DAY_MS = 24 * 60 * 60 * 1000;

// ======================================================================
// ПОДКЛЮЧЕНИЕ (без ATTACH)
// ======================================================================
async function connectToDb(db) {
  const conn = await db.connect();
  await conn.run(`FORCE INSTALL quack;`).catch(() => {});
  await conn.run(`LOAD quack;`);
  // ATTACH не нужен — всё через quack_query
  return conn;
}

async function createDbAndConn() {
  const extPath = path.join(__dirname, 'node_modules/@duckdb/extensions');
  const db = await DuckDBInstance.create(':memory:', {
    extension_directory: extPath,
  });
  const conn = await connectToDb(db);
  return { db, conn };
}

// ======================================================================
// ЭКРАНИРОВАНИЕ SQL ДЛЯ quack_query
// ======================================================================
function escapeSqlForQuack(sql) {
  return sql.replace(/'/g, "''");
}

// ======================================================================
// ВЫПОЛНЕНИЕ ЧЕРЕЗ quack_query
// ======================================================================
async function runQuackQuery(conn, sql) {
  const escaped = escapeSqlForQuack(sql);
  const outerSql = `
    SELECT * FROM quack_query(
      '${QUACK_URL}',
      '${escaped}',
      token := '${QUACK_TOKEN}',
      disable_ssl := true
    );
  `;
  const prepared = await conn.prepare(outerSql);
  const result = await prepared.run();
  return await result.getRowObjects();
}

// ======================================================================
// ТАЙМАУТ
// ======================================================================
function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`TIMEOUT ${ms} мс: ${label}`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// ======================================================================
// ПАРСИНГ МЕТРИК
// ======================================================================
function parsePlanMetrics(planText) {
  const m = {
    scanRows: null, scanTimeMs: null,
    filterRows: null, filterTimeMs: null,
    aggregateRows: null, aggregateTimeMs: null,
    sortRows: null, sortTimeMs: null,
    totalTimeMs: null,
  };
  for (const line of planText.split('\n')) {
    const lower = line.toLowerCase();
    const timingMatch = line.match(/(?:timing[=:]\s*)?([\d.]+)\s*s\b/i);
    const rowsMatch =
      line.match(/([\d,]+)\s+rows?/i) ||
      line.match(/rows[=:]\s*(\d+)/i);

    if (lower.includes('table_scan')) {
      if (rowsMatch) m.scanRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) m.scanTimeMs = parseFloat(timingMatch[1]) * 1000;
    }
    if (lower.includes('hash_group_by') || lower.includes('ungrouped_aggregate')) {
      if (rowsMatch) m.aggregateRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) m.aggregateTimeMs = parseFloat(timingMatch[1]) * 1000;
    }
    if (lower.includes('order_by') || lower.includes('top_n')) {
      if (rowsMatch) m.sortRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) m.sortTimeMs = parseFloat(timingMatch[1]) * 1000;
    }
    if (lower.includes('filter')) {
      if (rowsMatch) m.filterRows = parseInt(rowsMatch[1].replace(/,/g, ''), 10);
      if (timingMatch) m.filterTimeMs = parseFloat(timingMatch[1]) * 1000;
    }
    const totalMatch = line.match(/Total Time:\s*([\d.]+)\s*s/i);
    if (totalMatch) m.totalTimeMs = parseFloat(totalMatch[1]) * 1000;
  }
  return m;
}

// ======================================================================
// CONN MANAGER (упрощён: без ATTACH)
// ======================================================================
class ConnManager {
  constructor(db, conn) { this.db = db; this.conn = conn; }
  async reset() {
    try { this.conn.closeSync(); } catch (_) {}
    try { this.db.closeSync?.(); } catch (_) {}
    const { db, conn } = await createDbAndConn();
    this.db = db; this.conn = conn;
    console.warn('[ConnManager] Соединение пересоздано.');
  }
}

// ======================================================================
// БАЗОВЫЕ ФУНКЦИИ ТЕСТОВ (через quack_query)
// ======================================================================
async function aggregateTest(cm, label, sql) {
  console.log(`\n${'='.repeat(74)}`);
  console.log(`ТЕСТ: ${label}`);
  console.log(`SQL: ${sql}`);
  console.log('='.repeat(74));
  const explainSql = `EXPLAIN ANALYZE ${sql}`;
  let planText = '';
  try {
    const rows = await withTimeout(
      runQuackQuery(cm.conn, explainSql),
      QUERY_TIMEOUT_MS, label
    );
    for (const row of rows)
      for (const [, val] of Object.entries(row))
        if (typeof val === 'string') planText += val + '\n';
    console.log('--- ПЛАН ---');
    console.log(planText);
  } catch (err) {
    console.error(`ОШИБКА в тесте "${label}":`, err.message);
    return null;
  }
  const m = parsePlanMetrics(planText);
  console.log('--- МЕТРИКИ ---');
  console.log(`  TABLE_SCAN rows:    ${m.scanRows ?? 'n/a'}`);
  console.log(`  TABLE_SCAN time:    ${m.scanTimeMs ?? 'n/a'} ms`);
  console.log(`  FILTER rows:        ${m.filterRows ?? 'n/a'}`);
  console.log(`  AGGREGATE rows:     ${m.aggregateRows ?? 'n/a'}`);
  console.log(`  SORT rows:          ${m.sortRows ?? 'n/a'}`);
  console.log(`  TOTAL time:         ${m.totalTimeMs ?? 'n/a'} ms`);
  return m;
}

async function timedTest(cm, label, sql) {
  console.log(`\n${'='.repeat(74)}`);
  console.log(`TIMED-ТЕСТ: ${label}`);
  console.log(`SQL: ${sql}`);
  console.log('='.repeat(74));
  try {
    const t0 = Date.now();
    const rows = await withTimeout(
      runQuackQuery(cm.conn, sql),
      QUERY_TIMEOUT_MS, label
    );
    const ms = Date.now() - t0;
    console.log(`  Выполнено за ${ms} мс, строк: ${rows.length}`);
    if (rows.length <= 30) console.table(rows);
    else console.log('  Первые 5 строк:', rows.slice(0, 5));
    return { ms, rows };
  } catch (err) {
    console.error(`ОШИБКА в timed-тесте "${label}":`, err.message);
    return null;
  }
}

async function parquetTest(cm, label, sql) {
  console.log(`\n${'='.repeat(74)}`);
  console.log(`PARQUET-ТЕСТ: ${label}`);
  console.log(`SQL: ${sql}`);
  console.log('='.repeat(74));
  // COPY выполняется НА СЕРВЕРЕ quack, файл создаётся на сервере.
  const parquetPath = `${TMP_DIR}/agg_${Date.now()}_${Math.random().toString(36).slice(2)}.parquet`;
  const copySql = `COPY (${sql}) TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);`;
  try {
    const t0 = Date.now();
    await withTimeout(
      runQuackQuery(cm.conn, copySql),
      PARQUET_TIMEOUT_MS, label
    );
    const ms = Date.now() - t0;
    console.log(`  COPY завершён за ${ms} мс (файл на СЕРВЕРЕ): ${parquetPath}`);
    // Проверить наличие файла с клиента нельзя — он на сервере.
    return ms;
  } catch (err) {
    console.error(`ОШИБКА в Parquet-тесте "${label}":`, err.message);
    return null;
  }
}

// ======================================================================
// ОПИСАНИЕ ТЕСТОВ
// ======================================================================
function buildTests() {
  return [
    {
      name: 'A1: EXPLAIN ANALYZE — минутные агрегаты за сутки',
      enabled: true,
      run: (cm, ctx) => aggregateTest(cm,
        'A1: минутные агрегаты за сутки',
        `SELECT (ts // ${BUCKET_1MIN * 1000}) AS bucket_min, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_min, id
         ORDER BY bucket_min, id;`
      ),
    },
    {
      name: 'A2: timed — минутные агрегаты за сутки',
      enabled: true,
      run: (cm, ctx) => timedTest(cm,
        'A2: минутные агрегаты за сутки',
        `SELECT (ts // ${BUCKET_1MIN * 1000}) AS bucket_min, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_min, id
         ORDER BY bucket_min, id;`
      ),
    },
    {
      name: 'A3: Parquet — минутные агрегаты за сутки',
      enabled: false,
      run: (cm, ctx) => parquetTest(cm,
        'A3: минутные агрегаты за сутки',
        `SELECT (ts // ${BUCKET_1MIN * 1000}) AS bucket_min, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_min, id
         ORDER BY bucket_min, id`
      ),
    },
    {
      name: 'B1: EXPLAIN ANALYZE — 10-минутные агрегаты за сутки',
      enabled: true,
      run: (cm, ctx) => aggregateTest(cm,
        'B1: 10-минутные агрегаты за сутки',
        `SELECT (ts // ${BUCKET_10MIN * 1000}) AS bucket_10min, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_10min, id
         ORDER BY bucket_10min, id;`
      ),
    },
    {
      name: 'B2: timed — 10-минутные агрегаты за сутки',
      enabled: true,
      run: (cm, ctx) => timedTest(cm,
        'B2: 10-минутные агрегаты за сутки',
        `SELECT (ts // ${BUCKET_10MIN * 1000}) AS bucket_10min, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_10min, id
         ORDER BY bucket_10min, id;`
      ),
    },
    {
      name: 'C1: EXPLAIN ANALYZE — часовые агрегаты за сутки',
      enabled: true,
      run: (cm, ctx) => aggregateTest(cm,
        'C1: часовые агрегаты за сутки',
        `SELECT (ts // ${BUCKET_1HOUR * 1000}) AS bucket_hour, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_hour, id
         ORDER BY bucket_hour, id;`
      ),
    },
    {
      name: 'C2: timed — часовые агрегаты за сутки',
      enabled: true,
      run: (cm, ctx) => timedTest(cm,
        'C2: часовые агрегаты за сутки',
        `SELECT (ts // ${BUCKET_1HOUR * 1000}) AS bucket_hour, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_hour, id
         ORDER BY bucket_hour, id;`
      ),
    },
    {
      name: 'D1: EXPLAIN ANALYZE — сырые данные за сутки, LIMIT 10000',
      enabled: true,
      run: (cm, ctx) => aggregateTest(cm,
        'D1: сырые данные за сутки, LIMIT 10000',
        `SELECT ts, id, val, q FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         ORDER BY ts DESC LIMIT 10000;`
      ),
    },
    {
      name: 'D2: timed — сырые данные за сутки, LIMIT 10000',
      enabled: true,
      run: (cm, ctx) => timedTest(cm,
        'D2: сырые данные за сутки, LIMIT 10000',
        `SELECT ts, id, val, q FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         ORDER BY ts DESC LIMIT 10000;`
      ),
    },
    {
      name: 'D3: timed — COUNT(*) сырых за сутки',
      enabled: true,
      run: (cm, ctx) => timedTest(cm,
        'D3: COUNT(*) сырых за сутки',
        `SELECT COUNT(*) AS cnt FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList});`
      ),
    },
    {
      name: 'E1: Parquet — часовые агрегаты за сутки',
      enabled: false,
      run: (cm, ctx) => parquetTest(cm,
        'E1: часовые агрегаты за сутки',
        `SELECT (ts // ${BUCKET_1HOUR * 1000}) AS bucket_hour, id,
                COUNT(*) AS cnt, SUM(val) AS total, AVG(val) AS avg_val
         FROM ${TABLE}
         WHERE ts >= ${ctx.from24h} AND id IN (${idList})
         GROUP BY bucket_hour, id
         ORDER BY bucket_hour, id`
      ),
    },
  ];
}

// ======================================================================
// ОСНОВНОЙ СЦЕНАРИЙ
// ======================================================================
async function main() {
  const { db, conn } = await createDbAndConn();
  const cm = new ConnManager(db, conn);

  try {
    // 0. Подготовка: схема и серверное «сейчас» через quack_query
    console.log(`\n=== ПОДГОТОВКА ===`);
    let nowMs;
    try {
      const schema = await withTimeout(
        runQuackQuery(cm.conn, `DESCRIBE ${TABLE};`),
        QUERY_TIMEOUT_MS, 'DESCRIBE'
      );
      console.table(schema);

      const nowRow = await withTimeout(
        runQuackQuery(cm.conn, `SELECT epoch_ms(now()) AS now_ms;`),
        QUERY_TIMEOUT_MS, 'epoch_ms(now())'
      );
      nowMs = Number(nowRow[0].now_ms);
      console.log(`Серверное "сейчас": ${nowMs}`);
    } catch (err) {
      console.error('ОШИБКА подготовки:', err.message);
      return;
    }

    const ctx = { nowMs, from24h: nowMs - DAY_MS };

    // Прогон тестов
    const tests = buildTests();
    const summary = [];

    for (const test of tests) {
      if (!test.enabled) {
        console.log(`\n[SKIP] ${test.name}`);
        continue;
      }

      // reset перед каждым тестом оставлен для чистоты,
      // хотя quack_query stateless и не должен накапливать состояние.
      console.log(`\n[RESET] Пересоздание соединения перед "${test.name}"...`);
      try {
        await cm.reset();
      } catch (err) {
        console.error(`[RESET FAIL] ${err.message}`);
        continue;
      }

      const t0 = Date.now();
      let result = null;
      try {
        result = await test.run(cm, ctx);
      } catch (err) {
        console.error(`[FAIL] ${test.name}:`, err.message);
      }
      const elapsed = Date.now() - t0;
      summary.push({ test: test.name, elapsed, result });
    }

    // Итог
    console.log(`\n${'='.repeat(74)}`);
    console.log('ИТОГ');
    console.log('='.repeat(74));
    for (const s of summary) {
      const rows = s.result?.rows?.length ?? s.result?.aggregateRows ?? 'n/a';
      console.log(`  ${s.test}: ${s.elapsed} мс, строк: ${rows}`);
    }
  } finally {
    try { cm.conn.closeSync(); } catch (_) {}
    try { cm.db.closeSync?.(); } catch (_) {}
  }
}

main().catch((err) => {
  console.error('FATAL:', err);
  process.exit(1);
});