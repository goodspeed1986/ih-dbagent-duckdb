/**
 * DuckDB клиент на базе @duckdb/node-api
 * + pg-style совместимость
 * + prepared cache
 * + ускоренные вставки в records / strrecords / staterecords
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
            memory_limit: dbopt.memLimit ? dbopt.memLimit + 'GB' : '4GB',
            preserve_insertion_order: dbopt.preserve,
        });
        this.conn = await this.instance.connect();

        await this.conn.run(`LOAD quack;`);
        logger.log('✅ Quack extension загружен');

        await this.conn.run(`SET allocator_background_threads = true;`);
        logger.log('✅ SET allocator_background_threads');
        await this.conn.run(`SET async_threads = 32;`);
        logger.log('✅ SET async_threads');

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
     * Универсальный быстрый insert массивов объектов.
     * Для специализированных таблиц (records / strrecords / staterecords)
     * лучше вызывать соответствующие методы — они идут через Appender.
     */
    async insertMany(tableName, rows) {
        if (!rows?.length) return { rowCount: 0 };
        const sql = `INSERT INTO ${tableName} SELECT * FROM unnest($1)`;
        return this.run(sql, [rows]);
    },

    /**
     * records: ts BIGINT, id INTEGER, val REAL, q INTEGER
     */
    async insertRecords(records) {
        if (!records?.length) return { rowCount: 0 };

        const appender = await this.conn.createAppender('records');
        try {
            for (const r of records) {
                appender.appendBigInt(r.ts != null ? BigInt(r.ts) : null);
                appender.appendInteger(r.id != null ? Number(r.id) : null);
                appender.appendDouble(r.val != null ? Number(r.val) : null);
                appender.appendInteger(r.q != null ? Number(r.q) : null);
                appender.endRow();
            }
            appender.flushSync();
            return { rowCount: records.length };
        } finally {
            appender.closeSync();
        }
    },

    async insertStrRecords(strs) {
        if (!strs?.length) return { rowCount: 0 };

        const isPlainStrings = typeof strs[0] === 'string';

        if (isPlainStrings) {
            const appender = await this.conn.createAppender('strrecords');
            try {
                for (const s of strs) {
                    appender.appendNull();          // ts
                    appender.appendNull();          // id
                    appender.appendVarchar(s != null ? String(s) : null); // val
                    appender.appendNull();          // q
                    appender.endRow();
                }
                appender.flushSync();
                return { rowCount: strs.length };
            } finally {
                appender.closeSync();
            }
        }

        const appender = await this.conn.createAppender('strrecords');
        try {
            for (const r of strs) {
                appender.appendBigInt(r.ts != null ? BigInt(r.ts) : null);
                appender.appendInteger(r.id != null ? Number(r.id) : null);
                appender.appendVarchar(r.val != null ? String(r.val) : null);
                appender.appendInteger(r.q != null ? Number(r.q) : null);
                appender.endRow();
            }
            appender.flushSync();
            return { rowCount: strs.length };
        } finally {
            appender.closeSync();
        }
    },

    async insertStateRecords(rows) {
        if (!rows?.length) return { rowCount: 0 };

        const appender = await this.conn.createAppender('staterecords');
        try {
            for (const r of rows) {
                appender.appendBigInt(r.ts != null ? BigInt(r.ts) : null);
                appender.appendInteger(r.id != null ? Number(r.id) : null);
                appender.appendInteger(r.val != null ? Number(r.val) : null);
                appender.appendInteger(r.q != null ? Number(r.q) : null);
                appender.endRow();
            }
            appender.flushSync();
            return { rowCount: rows.length };
        } finally {
            appender.closeSync();
        }
    },
    /**
     * Максимально быстрый вариант через Appender (для 500к+ строк).
     * rows = массив массивов в порядке колонок таблицы records.
     */
    async fastInsertAppender(tableName, rows) {
        if (!rows?.length) return;

        const appender = await this.conn.createAppender(tableName);

        try {
            for (const row of rows) {
                appender.appendBigInt(row[0]);  // ts — BIGINT
                appender.appendInteger(row[1]); // id — INTEGER
                appender.appendDouble(row[2]);  // val — REAL
                appender.appendInteger(row[3]); // q — INTEGER
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