/**
 * DuckDB клиент на базе @duckdb/node-api
 * + pg-style совместимость
 * + prepared cache
 * + ускоренные вставки в records / strrecords
 * + защита от раздувания памяти при больших выборках
 */

const { DuckDBInstance } = require('@duckdb/node-api');
const util = require('util');
const utils = require('./utils');
const path = require('path');
const fs = require('fs');

module.exports = {
  db: null,
  conn: null,
  pool: null,
  logger: console,

  // Кэш prepared statements
  preparedCache: new Map(),

  async createPoolToDatabase(dbopt = {}, logger = console) {
    this.logger = logger;
    const folder = path.dirname(dbopt.dbPath);
    if (!fs.existsSync(folder)) {
      fs.mkdirSync(folder, { recursive: true });
    }

    const extPath = path.join(__dirname, '..', 'node_modules/@duckdb/extensions');

    this.instance = await DuckDBInstance.create(dbopt.dbPath, {
      threads: dbopt.threads || 8,
      access_mode: 'READ_WRITE',
      extension_directory: extPath,
      memory_limit: dbopt.memLimit ? dbopt.memLimit + "GB" : '4GB',   // раскомментируй при необходимости
      preserve_insertion_order: dbopt.preserve
    });
    this.conn = await this.instance.connect();

    await this.conn.run(`LOAD quack;`);
    logger.log('✅ Quack extension загружен');

    await this.conn.run(`SET allocator_background_threads = true;`);
    logger.log('✅ SET allocator_background_threads');
    await this.conn.run(`SET async_threads = 32;`);
    logger.log('✅ SET async_threads');
    /*await this.conn.run(`SET preserve_insertion_order = false;`);
    logger.log('✅ SET preserve_insertion_order');

    // Ограничиваем память DuckDB (можно вынести в dbopt)
    try {
      await this.conn.run(`SET memory_limit = '4GB';`);
      logger.log('✅ memory_limit = 4GB');
    } catch (e) {
      logger.warn('Не удалось установить memory_limit:', e.message);
    }*/

    const serveResult = await this.conn.runAndReadAll(`
      CALL quack_serve('quack:localhost:9495', token = 'Intra_secret');
    `);
    logger.log('🚀 Quack Server запущен на quack://0.0.0.0:9495');

    // ──────── Фейковый pg-pool ────────
    this.pool = {
      query: async (sqlOrConfig, values, callback) => {
        const text = typeof sqlOrConfig === 'string' ? sqlOrConfig : sqlOrConfig.text;
        let params = values || (sqlOrConfig?.values ?? []);
        if (!Array.isArray(params)) params = [];

        try {
          let rows;
          if (params.length > 0) {
            const prepared = await this.conn.prepare(text);
            for (let i = 0; i < params.length; i++) {
              await prepared.bind(i + 1, params[i]);
            }
            const result = await prepared.run();
            rows = await result.getRowObjects();
          } else {
            const reader = await this.conn.runAndReadAll(text);
            rows = reader.getRowObjects();
          }

          utils.convertBigint(rows);

          const res = {
            rows,
            rowCount: rows.length,
            command: text.trim().split(/\s+/)[0].toUpperCase(),
          };

          if (callback) callback(null, res);
          return res;
        } catch (err) {
          if (callback) callback(err);
          throw err;
        }
      },

      end: () => {
        if (this.conn) {
          this.conn.closeSync();
          this.conn = null;
        }
      },
      close: async () => this.pool.end(),
    };

    this.run = async (sql, values = []) => this.pool.query(sql, values);
    this.query = async (sql, values = []) => (await this.pool.query(sql, values)).rows;

    this.logger.log(`DuckDB подключение создано`);
    return this.pool;
  },

  // ==================== УСКОРЕННЫЕ ВСТАВКИ ====================

  /**
   * Универсальный быстрый insert массивов объектов
   * Работает через UNNEST → DuckDB сам матчит поля по именам
   */
  async insertMany(tableName, rows) {
    if (!rows?.length) return { rowCount: 0 };

    const sql = `INSERT INTO ${tableName} SELECT * FROM unnest($1)`;
    return this.run(sql, [rows]);
  },

  /**
   * Специально для таблицы records (массив объектов)
   */
  async insertRecords(records) {
    return this.insertMany('records', records);
  },

  /**
   * Специально для таблицы strrecords (массив строк)
   */
  async insertStrRecords(strs) {
    if (!strs?.length) return { rowCount: 0 };

    const sql = `INSERT INTO strrecords SELECT unnest($1::VARCHAR[])`;
    return this.run(sql, [strs]);
  },

  /**
   * Максимально быстрый вариант через Appender (для 500к+ строк)
   * rows = массив массивов в порядке колонок таблицы
   */
  async fastInsertAppender(tableName, rows) {
    if (!rows?.length) return;

    const appender = await this.conn.createAppender(tableName);

    try {
      for (const row of rows) {
        appender.appendBigInt(row[0]);    // ts — BIGINT
        appender.appendInteger(row[1]);   // id — INTEGER
        appender.appendDouble(row[2]);    // val — REAL
        appender.appendInteger(row[3]);   // q — INTEGER
        appender.endRow();
      }
      appender.flushSync();
    } finally {
      appender.closeSync();
    }
  },

  /**
   * Безопасный способ выполнить тяжёлую агрегацию и сразу записать результат в Parquet.
   * Не возвращает миллионы строк в Node.
   */
  async queryToParquet(sql, values = [], outputPath = null) {
    const filePath = outputPath || `/tmp/duckdb_agg_${Date.now()}.parquet`;

    const copySql = `
      COPY (
        ${sql}
      ) TO '${filePath}' (FORMAT PARQUET, COMPRESSION ZSTD);
    `;

    await this.run(copySql, values);
    return { filePath, message: 'Результат записан в Parquet' };
  },

  async end() {
    this.preparedCache.clear();
    if (this.pool) await this.pool.end();
  },
};