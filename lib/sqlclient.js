const { DuckDBInstance } = require('@duckdb/node-api');
const utils = require('./utils');
const path = require('path');

class Sqlclient {
  constructor(opt) {
    if (opt.bxpwd) {
      opt.password = utils.decryptBx(opt.bxpwd);
    }
    this.opt = opt;
    this.db = null;
    this.conn = null;
    this.quackUri = 'quack:localhost:9495';
    this.quackToken = 'Intra_secret';
    this.quackDisableSsl = true;
  }

  async connect() {
    try {
      const extPath = path.join(
        __dirname,
        '..',
        'node_modules/@duckdb/extensions'
      );
      this.db = await DuckDBInstance.create(':memory:', {
        extension_directory: extPath,
      });
      this.conn = await this.db.connect();
      //await this.conn.run(`FORCE INSTALL quack;`).catch(() => {});
      await this.conn.run(`LOAD quack;`);
    } catch (err) {
      console.error('ERROR MY', err);
      throw new Error(`DuckDB connection failed: ${err.message}`);
    }
  }

  /**
   * Подготовка SQL-строки из queryObj.
   * Возвращает SQL-строку (без префикса remote_db. —
   * через quack_query префикс не нужен).
   */
  prepareQuery(queryObj) {
    let queryStr;
    if (typeof queryObj === 'string') {
      queryStr = queryObj;
    } else if (queryObj.sql) {
      queryStr = queryObj.sql;
    } else {
      if (!queryObj.ids) return '';
      const idarr = Array.isArray(queryObj.ids)
        ? queryObj.ids
        : queryObj.ids.split(',');
      queryStr = utils.getQueryStrId(queryObj, idarr);
    }
    return queryStr;
  }

  /**
   * Выполнение SQL через quack_query.
   * SQL передаётся строкой, bind-параметры не поддерживаются.
   */
  async _quackQuery(sql) {
    const escapedSql = sql.replace(/'/g, "''");
    const outerSql = `
      SELECT * FROM quack_query(
        '${this.quackUri}',
        '${escapedSql}',
        token := '${this.quackToken}',
        disable_ssl := ${this.quackDisableSsl}
      );
    `;
    const prepared = await this.conn.prepare(outerSql);
    const result = await prepared.run();
    return await result.getRowObjects();
  }

  /**
   * Обычный query через quack_query.
   * Возвращает массив строк. BigInt конвертируется в Number.
   */
  async query(sql) {
    if (!sql || typeof sql !== 'string') {
      throw new Error('Expected query as SQL string!');
    }

    let rows;
    try {
      rows = await this._quackQuery(sql);
    } catch (err) {
      throw new Error(`Query failed: ${err.message}\nSQL: ${sql}`);
    }

    // Конвертация BigInt на месте
    if (rows.length > 0) {
      const bigintKeys = Object.keys(rows[0]).filter(
        (key) => typeof rows[0][key] === 'bigint'
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
  }

  /**
   * Вставка записей через quack_query.
   */
  async insertRecords(tableName, records) {
    if (!records || records.length === 0) return { rowCount: 0 };

    try {
      const columns = Object.keys(records[0]);
      let insertedCount = 0;

      for (const record of records) {
        const valuesSql = columns
          .map((col) => this._toSqlLiteral(record[col]))
          .join(', ');
        const insertSql = `INSERT INTO ${tableName} (${columns.join(
          ', '
        )}) VALUES (${valuesSql})`;
        await this._quackQuery(insertSql);
        insertedCount++;
      }

      return { rowCount: insertedCount };
    } catch (err) {
      throw new Error(`Bulk insert failed: ${err.message}`);
    }
  }

  /**
   * Последовательное выполнение нескольких SQL.
   * ВНИМАНИЕ: quack_query stateless, атомарной транзакции нет.
   */
  async transaction(queries) {
    try {
      for (const queryItem of queries) {
        const queryStr =
          typeof queryItem === 'string' ? queryItem : queryItem.text;
        await this._quackQuery(queryStr);
      }
      return { success: true };
    } catch (err) {
      throw new Error(`Transaction failed: ${err.message}`);
    }
  }

  /**
   * Преобразование JS-значения в SQL-литерал.
   */
  _toSqlLiteral(v) {
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number') return String(v);
    if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE';
    if (typeof v === 'bigint') return String(v);
    return `'${String(v).replace(/'/g, "''")}'`;
  }

  close() {
    if (this.conn) {
      try {
        this.conn.closeSync();
      } catch (_) {
        /* ignore */
      }
      this.conn = null;
    }
    this.db = null;
  }
}

module.exports = Sqlclient;