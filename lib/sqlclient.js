const { DuckDBInstance } = require('@duckdb/node-api');
const utils = require('./utils');
const util = require("util")

class Sqlclient {
  constructor(opt) {
    if (opt.bxpwd) {
      opt.password = utils.decryptBx(opt.bxpwd);
    }
    this.opt = opt;
    this.db = null;
    this.conn = null;
    this.pool = null;
  }

  async connect() {
    try {
      this.db = await DuckDBInstance.create(':memory:');
      this.conn = await this.db.connect();

      await this.conn.run(`
        CREATE SECRET (
          TYPE quack,
          TOKEN 'Intra_secret'
        );
      `);

      await this.conn.run(`
        ATTACH 'quack:localhost:9495' AS remote_db;
      `);
      console.log("connected")
      // Для совместимости с pg-стилем
      this.pool = {
        query: async (config, callback) => {
          let text = typeof config === 'string' ? config : config.text;
          const values = config.values || [];

          // Автоматически добавляем remote_db. к таблицам
          text = this.addRemotePrefix(text);

          try {
            const prepared = await this.conn.prepare(text);

            for (let i = 0; i < values.length; i++) {
              await prepared.bind(i + 1, values[i]);
            }

            const result = await prepared.run();
            const rows = await result.getRowObjects();
            const res = {
              rows: rows,
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
   * Добавляет префикс remote_db. ко всем таблицам в FROM/JOIN
   */
  addRemotePrefix(sql) {
    if (!sql || typeof sql !== 'string') return sql;

    // Более надёжная регулярка для FROM и JOIN
    // Обрабатывает: FROM table, FROM table alias, JOIN table, etc.
    return sql.replace(
      /\b(FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?=\s|$|\(|\,|\s+AS|\s+ON|\s+WHERE)/gi,
      (match, keyword, table) => {
        // Не добавляем префикс, если уже есть remote_db. или если это функция/подзапрос
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

    // Применяем префикс и здесь
    return this.addRemotePrefix(queryStr);
  }

  async query(queryStr, values = []) {
    if (!queryStr) throw new Error('Empty queryStr!');
    if (typeof queryStr !== 'string') throw new Error('Expected query as SQL string!');

    // Добавляем префикс
    const processedQuery = this.addRemotePrefix(queryStr);
    try {
      const prepared = await this.conn.prepare(processedQuery);

      for (let i = 0; i < values.length; i++) {
        await prepared.bind(i + 1, values[i]);
      }

      const result = await prepared.run();
      const rows = await result.getRowObjects();
      if (rows.length > 0) {
        // Находим все bigint-поля один раз
        const bigintKeys = Object.keys(rows[0]).filter(
          key => typeof rows[0][key] === 'bigint'
        );

        // Конвертируем только эти поля во всех строках
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

  // Остальные методы тоже должны использовать префикс
  async insertRecords(tableName, records) {
    if (!records || records.length === 0) return { rowCount: 0 };

    // Для INSERT используем полное имя remote_db.tableName
    const fullTable = tableName.includes('.') ? tableName : `remote_db.${tableName}`;

    try {
      const columns = Object.keys(records[0]);
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');

      const query = `INSERT INTO ${fullTable} (${columns.join(', ')}) VALUES (${placeholders})`;
      const prepared = await this.conn.prepare(query);

      let insertedCount = 0;
      for (const record of records) {
        const values = columns.map(col => record[col]);
        for (let i = 0; i < values.length; i++) {
          await prepared.bind(i + 1, values[i]);
        }
        await prepared.run();
        insertedCount++;
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
      const prepared = await this.conn.prepare(query);

      let insertedCount = 0;
      for (const record of records) {
        for (let i = 0; i < record.length; i++) {
          await prepared.bind(i + 1, record[i]);
        }
        await prepared.run();
        insertedCount++;
      }

      return { rowCount: insertedCount };
    } catch (err) {
      throw new Error(`Bulk str insert failed: ${err.message}`);
    }
  }

  async transaction(queries) {
    try {
      await this.conn.query('BEGIN TRANSACTION');

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

      await this.conn.query('COMMIT');
      return { success: true };
    } catch (err) {
      await this.conn.query('ROLLBACK');
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