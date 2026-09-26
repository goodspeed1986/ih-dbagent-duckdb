/**
 * DuckDB клиент на базе @duckdb/node-api
 * + pg-style совместимость
 * + prepared cache
 * + ускоренные вставки в records / strrecords
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
        extension_directory: extPath
    });
    this.conn = await this.instance.connect();

    await this.conn.run(`LOAD quack;`);
    logger.log('✅ Quack extension загружен');

    const serveResult = await this.conn.runAndReadAll(`
        CALL quack_serve('quack:localhost:9495', token = 'Intra_secret');
    `);
    //console.log('Сервер запущен. Ответ:', serveResult.getRowObjects());
    logger.log('🚀 Quack Server запущен на quack://0.0.0.0:9495');

    // ──────── Фейковый pg-pool ────────
    this.pool = {
        query: async (sqlOrConfig, values, callback) => {
            const text = typeof sqlOrConfig === 'string' ? sqlOrConfig : sqlOrConfig.text;
            let params = values || (sqlOrConfig?.values ?? []);
            if (!Array.isArray(params)) params = [];

            try {
                // Без кеша: prepare каждый раз внутри conn.run/prepare
                let rows;
                if (params.length > 0) {
                    // Есть параметры — используем prepare + bind
                    const prepared = await this.conn.prepare(text);
                    for (let i = 0; i < params.length; i++) {
                        await prepared.bind(i + 1, params[i]);
                    }
                    const result = await prepared.run();
                    rows = await result.getRowObjects();
                } else {
                    // Без параметров — runAndReadAll быстрее
                    const reader = await this.conn.runAndReadAll(text);
                    rows = reader.getRowObjects();
                }

                utils.convertBigint2Number(rows);

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
    return this.run(sql, [rows]); // rows — массив объектов
  },

  /**
   * Специально для таблицы records (массив объектов)
   */
  async insertRecords(records) {
    return this.insertMany('records', records);
  },

  /**
   * Специально для таблицы strrecords (массив строк)
   * Предполагается колонка типа VARCHAR (или TEXT)
   */
  async insertStrRecords(strs) {
    if (!strs?.length) return { rowCount: 0 };

    // Если колонка называется "value" или "str" — поменяй
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
            // row = [ts, id, val, q]
            appender.appendBigInt(row[0]);    // ts — BIGINT
            appender.appendInteger(row[1]);    // id — INTEGER
            appender.appendDouble(row[2]);     // val — REAL
            appender.appendInteger(row[3]);    // q — INTEGER
            appender.endRow();
        }
        appender.flushSync();
    } finally {
        appender.closeSync();
    }
},

  async end() {
    this.preparedCache.clear();
    if (this.pool) await this.pool.end();
  },
};