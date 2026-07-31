/**
 * DuckDB клиент на базе @duckdb/node-api
 * + pg-style совместимость
 * + prepared cache
 * + ускоренные вставки в records / strrecords
 */

const { DuckDBInstance } = require('@duckdb/node-api');
const util = require('util');

module.exports = {
  db: null,
  conn: null,
  pool: null,
  logger: console,

  // Кэш prepared statements
  preparedCache: new Map(),

  async createPoolToDatabase(dbopt = {}, logger = console) {
    this.logger = logger;
    const path = dbopt.database || ':memory:';
    this.logger.log("path " + path)
    this.instance = await DuckDBInstance.create('test_duckdb', {
      threads: dbopt.threads || 8,
      access_mode: 'READ_WRITE'
    });
    this.conn = await this.instance.connect();
    // Установка и загрузка расширения
    await this.conn.run(`SET extension_directory = './extensions';`);
    await this.conn.run(`SET home_directory = './duckdb_home';`);
    await this.conn.run(`INSTALL quack FROM core_nightly;`);
    await this.conn.run(`LOAD quack;`);

    logger.log('✅ Quack extension загружен');

    // Правильный запуск сервера
    await this.conn.run(`
    CALL quack_serve('quack:localhost:9495', 
                     token = 'Intra_secret',
                     allow_other_hostname = true);
  `);
  logger.log('🚀 Quack Server запущен на quack://0.0.0.0:9495');
    // ──────── Фейковый pg-pool ────────
    this.pool = {
      query: async (sqlOrConfig, values, callback) => {
        const text = typeof sqlOrConfig === 'string' ? sqlOrConfig : sqlOrConfig.text;
        let params = values || (sqlOrConfig?.values ?? []);

        if (!Array.isArray(params)) params = [];

        try {
          let prepared = this.preparedCache.get(text);
          if (!prepared) {
            prepared = await this.conn.prepare(text);
            this.preparedCache.set(text, prepared);
          }

          for (let i = 0; i < params.length; i++) {
            await prepared.bind(i + 1, params[i]);
          }

          const result = await prepared.run();
          const rows = await result.getRowObjects();

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
        this.preparedCache.clear();
        if (this.conn) {
          this.conn.closeSync();
          this.conn = null;
        }
      },
      close: async () => this.pool.end(),
    };

    this.run = async (sql, values = []) => this.pool.query(sql, values);
    this.query = async (sql, values = []) => (await this.pool.query(sql, values)).rows;

    this.logger.log(`DuckDB подключение создано → ${path}`);
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

    const appender = await this.conn.createAppender(tableName); // или ('main', tableName)

    try {
      for (const row of rows) {
        for (const val of row) {
          // Универсальный append (DuckDB сам кастует)
          if (typeof val === 'number') appender.appendInteger(val);
          else if (typeof val === 'string') appender.appendVarchar(val);
          else if (val === null || val === undefined) appender.appendNull();
          else appender.appendVarchar(String(val)); // fallback
        }
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