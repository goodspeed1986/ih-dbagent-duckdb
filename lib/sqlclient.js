const { DuckDBInstance } = require('@duckdb/node-api');
const utils = require('./utils');
const util = require("util");
const path = require('path');

class Sqlclient {
  constructor(opt) {
    if (opt.bxpwd) {
      opt.password = utils.decryptBx(opt.bxpwd);
    }
    this.opt = opt;
    this.db = null;
    this.conn = null;
    this.pool = null;
    this._reconnecting = null; // защита от параллельных реконнектов
  }

  async connect() {
    try {
      const extPath = path.join(
            __dirname,
            '..',
            'node_modules/@duckdb/extensions'
          );
      this.db = await DuckDBInstance.create(':memory:', {
        extension_directory: extPath
      });
      this.conn = await this.db.connect();
      await this.conn.run(`LOAD quack;`);
      await this.conn.run(`
        CREATE OR REPLACE SECRET (
          TYPE quack,
          TOKEN 'Intra_secret',
          SCOPE 'quack:localhost:9495'
        );
      `);
  
      await this.conn.run(`
        ATTACH 'quack:localhost:9495' AS remote_db (TYPE quack);
      `);

      this.pool = {
        query: async (config, callback) => {
          let text = typeof config === 'string' ? config : config.text;
          const values = config.values || [];

          text = this.addRemotePrefix(text);

          try {
            const rows = await this._runWithRetry(async (conn) => {
              const prepared = await conn.prepare(text);
              for (let i = 0; i < values.length; i++) {
                await prepared.bind(i + 1, values[i]);
              }
              const result = await prepared.run();
              return await result.getRowObjects();
            });

            const res = {
              rows: rows.map(row => ({
                ...row,
                ts: Number(row.ts)
              })),
              rowCount: rows.length,
              command: text.trim().split(/\s+/)[0].toUpperCase(),
            };

            if (callback) callback(null, res);
            return rows;
          } catch (err) {
            if (callback) callback(err);
            throw err;
          }
        },
        end: () => this.close()
      };

      return Promise.resolve();
    } catch (err) {
      console.error("ERROR MY", err);
      throw new Error(`DuckDB connection failed: ${err.message}`);
    }
  }

  /**
   * Проверка на ошибку устаревшего connection id
   */
  _isInvalidConnectionError(err) {
    if (!err) return false;
    const msg = (err.message || '').toLowerCase();
    return msg.includes('invalid connection id') ||
           msg.includes('invalid connection') ||
           msg.includes('connection id');
  }

  /**
   * Пересоздание сессии: DETACH + ATTACH на текущем conn.
   * Если и это не помогает — пересоздаём сам conn.
   */
  async _reconnect() {
    if (this._reconnecting) return this._reconnecting;

    this._reconnecting = (async () => {
      try {
        console.warn('[Sqlclient] Reconnecting: DETACH/ATTACH remote_db...');

        // 1) Пытаемся отцепить старую (битую) сессию
        try {
          await this.conn.run(`DETACH remote_db;`);
        } catch (e) {
          // Игнорируем: сессия уже мертва
        }

        // 2) Пробуем ATTACH заново
        try {
          await this.conn.run(`
            ATTACH 'quack:localhost:9495' AS remote_db (TYPE quack);
          `);
          console.warn('[Sqlclient] Reconnect OK (reuse conn).');
          return;
        } catch (attachErr) {
          console.warn('[Sqlclient] ATTACH failed, recreating connection:', attachErr.message);
        }

        // 3) Если ATTACH не удался — пересоздаём соединение целиком
        try {
          this.conn.closeSync();
        } catch (e) { /* ignore */ }

        this.conn = await this.db.connect();
        await this.conn.run(`LOAD quack;`);
        await this.conn.run(`
          CREATE OR REPLACE SECRET (
            TYPE quack,
            TOKEN 'Intra_secret',
            SCOPE 'quack:localhost:9495'
          );
        `);
        await this.conn.run(`
          ATTACH 'quack:localhost:9495' AS remote_db (TYPE quack);
        `);
        console.warn('[Sqlclient] Reconnect OK (new conn).');
      } finally {
        this._reconnecting = null;
      }
    })();

    return this._reconnecting;
  }

  /**
   * Универсальный ретрай для операций с conn.
   * fn(conn) => Promise<result>
   */
  async _runWithRetry(fn, retries = 1) {
    try {
      return await fn(this.conn);
    } catch (err) {
      if (this._isInvalidConnectionError(err) && retries > 0) {
        await this._reconnect();
        return this._runWithRetry(fn, retries - 1);
      }
      throw err;
    }
  }

  /**
   * Добавляет префикс remote_db. ко всем таблицам в FROM/JOIN
   */
  addRemotePrefix(sql) {
    if (!sql || typeof sql !== 'string') return sql;

    return sql.replace(
      /\b(FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?=\s|$|\(|\,|\s+AS|\s+ON|\s+WHERE)/gi,
      (match, keyword, table) => {
        if (table.toLowerCase() === 'remote_db' ||
          /\.|(select|values|\(|\)|,)/i.test(table)) {
          return match;
        }
        return `${keyword} remote_db.${table}`;
      }
    );
  }

  prepareQuery(queryObj) {
    let queryStr;
    if (typeof queryObj === 'string') {
      queryStr = queryObj;
    } else if (queryObj.sql) {
      queryStr = queryObj.sql;
    } else {
      if (!queryObj.ids) return '';
      const idarr = Array.isArray(queryObj.ids) ? queryObj.ids : queryObj.ids.split(',');
      queryStr = utils.getQueryStrId(queryObj, idarr);
    }

    return this.addRemotePrefix(queryStr);
  }

  async query(queryStr, values = []) {
    if (!queryStr) throw new Error('Empty queryStr!');
    if (typeof queryStr !== 'string') throw new Error('Expected query as SQL string!');

    const processedQuery = this.addRemotePrefix(queryStr);
    try {
      const rows = await this._runWithRetry(async (conn) => {
        const prepared = await conn.prepare(processedQuery);
        for (let i = 0; i < values.length; i++) {
          await prepared.bind(i + 1, values[i]);
        }
        const result = await prepared.run();
        return await result.getRowObjects();
      });

      if (rows.length > 0) {
        const bigintKeys = Object.keys(rows[0]).filter(
          key => typeof rows[0][key] === 'bigint'
        );

        if (bigintKeys.length > 0) {
          for (const item of rows) {
            for (const key of bigintKeys) {
              if (item[key] != null) {
                item[key] = Number(item[key]);
              }
            }
          }
        }
      }
      return rows;
    } catch (err) {
      throw new Error(`Query failed: ${err.message}\nSQL: ${processedQuery}`);
    }
  }

  async insertRecords(tableName, records) {
    if (!records || records.length === 0) return { rowCount: 0 };

    const fullTable = tableName.includes('.') ? tableName : `remote_db.${tableName}`;

    try {
      const columns = Object.keys(records[0]);
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
      const query = `INSERT INTO ${fullTable} (${columns.join(', ')}) VALUES (${placeholders})`;

      let insertedCount = 0;

      const runInsert = async (conn) => {
        const prepared = await conn.prepare(query);
        let count = 0;
        for (const record of records) {
          const values = columns.map(col => record[col]);
          for (let i = 0; i < values.length; i++) {
            await prepared.bind(i + 1, values[i]);
          }
          await prepared.run();
          count++;
        }
        return count;
      };

      try {
        insertedCount = await this._runWithRetry(runInsert);
      } catch (err) {
        // Если упали посреди батча — обнуляем счётчик, т.к. состояние неизвестно
        throw err;
      }

      return { rowCount: insertedCount };
    } catch (err) {
      throw new Error(`Bulk insert failed: ${err.message}`);
    }
  }

  async insertStrRecords(tableName, records) {
    if (!records || records.length === 0) return { rowCount: 0 };

    const fullTable = tableName.includes('.') ? tableName : `remote_db.${tableName}`;

    try {
      if (!Array.isArray(records[0])) {
        throw new Error('StrRecords must be an array of arrays');
      }

      const columnCount = records[0].length;
      const placeholders = records[0].map((_, i) => `$${i + 1}`).join(', ');
      const query = `INSERT INTO ${fullTable} VALUES (${placeholders})`;

      const runInsert = async (conn) => {
        const prepared = await conn.prepare(query);
        let count = 0;
        for (const record of records) {
          for (let i = 0; i < record.length; i++) {
            await prepared.bind(i + 1, record[i]);
          }
          await prepared.run();
          count++;
        }
        return count;
      };

      const insertedCount = await this._runWithRetry(runInsert);
      return { rowCount: insertedCount };
    } catch (err) {
      throw new Error(`Bulk str insert failed: ${err.message}`);
    }
  }

  async transaction(queries) {
    try {
      await this.conn.run('BEGIN TRANSACTION');

      for (const queryItem of queries) {
        let queryStr = typeof queryItem === 'string' ? queryItem : queryItem.text;
        const values = queryItem.values || [];
        queryStr = this.addRemotePrefix(queryStr);

        const prepared = await this.conn.prepare(queryStr);
        for (let i = 0; i < values.length; i++) {
          await prepared.bind(i + 1, values[i]);
        }
        await prepared.run();
      }

      await this.conn.run('COMMIT');
      return { success: true };
    } catch (err) {
      try { await this.conn.run('ROLLBACK'); } catch (e) { /* ignore */ }

      // Транзакцию нельзя «просто повторить» — пробрасываем ошибку.
      // Если это Invalid connection id — реконнектимся, чтобы следующий
      // запрос пользователя уже работал.
      if (this._isInvalidConnectionError(err)) {
        try { await this._reconnect(); } catch (e) { /* ignore */ }
      }

      throw new Error(`Transaction failed: ${err.message}`);
    }
  }

  close() {
    if (this.conn) {
      this.conn.closeSync();
      this.conn = null;
    }
    this.db = null;
    this.pool = null;
  }
}

module.exports = Sqlclient;