/**
 * dbagent main module
 */

const util = require('util');
const schedule = require('node-schedule');
const client = require('./client');
const utils = require('./utils');
const { promises: fs } = require('fs');

module.exports = async function (channel, opt, logger) {
  this.logger = logger;
  const options = getOptions(opt);
  let overflow = 0;
  let lastOverflow = 0;
  let maxTimeRead = 0;
  let maxTimeWrite = 0;

  let compressedTablesSet;

  let hoursRule = new schedule.RecurrenceRule();
  // hoursRule.rule = '*/15 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' }); // Get settings for retention policy
  });
  options.dbPath = options.projectPath + '/db/histduck.db';
  logger.log('Options: ' + JSON.stringify(options), 2);

  setInterval(async () => getDBSize(), 300000); // Get db size
  try {
    await client.createPoolToDatabase(options, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };


    await createTable('records');
    await client.query(`CREATE INDEX IF NOT EXISTS idx_records_ts ON records (id, ts DESC);`);


    await client.query(`CREATE SEQUENCE IF NOT EXISTS timeline_id_seq START 1;`);
    await createTable('timeline');
    await client.query(`CREATE INDEX IF NOT EXISTS idx_timeline_dn ON timeline (id, dn DESC);`);

    await client.query(`CREATE SEQUENCE IF NOT EXISTS customtable_id_seq START 1;`);
    await createTable('customtable');
    await client.query(`CREATE INDEX IF NOT EXISTS namex_customtable ON customtable (name DESC);`);

    await client.query(`CREATE SEQUENCE IF NOT EXISTS formulas_id_seq START 1;`);
    await createTable('formulas');

    await client.query(`CREATE SEQUENCE IF NOT EXISTS recids_id_seq START 1;`);
    await createTable('recids');

    await createTable('strrecords');
    await client.query(`CREATE INDEX IF NOT EXISTS idx_strrecords_ts ON strrecords (id, ts DESC);`);

    await createTable('staterecords');
    await client.query(`CREATE INDEX IF NOT EXISTS idx_staterecords_ts ON staterecords (id, ts DESC);`);

    getDBSize();


    channel.on('message', ({ id, type, query, payload, table }) => {
      if (type == 'write' && table == 'customtable') return writeCustom(id, payload, table);
      if (type == 'update') {
        return table == 'customtable' ? updateCustom(query, payload, table) : update(query, payload, table);
      }
      if (type == 'remove') return removeCustom(payload, table);
      if (type == 'removeall') return removeAll(table);

      if (type == 'write') {
        if (overflow == 0) return write(id, payload, table);
        if (overflow == 1 && lastOverflow == 0) {
          lastOverflow = overflow;
          return sendError(id, 'The allocated space for the database has run out, increase the limit');
        }
      }
      if (type == 'read') return read(id, query);
      if (type == 'settings') return del(payload);
      if (type == 'run') return run(id, query);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.close();
    });
  } catch (err) {
    processExit(1, err);
  }

  /**
   *
   * @param {String} tableName
   * @param {String} fname - optional
   */
  async function createTable(tableName) {
    return client.query(getCreateTableStr(tableName));
  }

  async function getDBSize() {
    try {
      let stats = await fs.stat(options.dbPath);
      let fileSize = stats.size / 1048576;
      stats = await fs.stat(options.dbPath + '.wal');
      fileSize += stats.size / 1048576;
      if (process.connected) process.send({ type: 'procinfo', data: { size: Math.round(fileSize * 100) / 100 } });
      overflow = fileSize > options.dbLimit ? 1 : 0;

      if (process.connected) process.send({ type: 'procinfo', data: { overflow } });
    } catch (e) {
      logger.log('getDBSize ERROR: ' + util.inspect(e));
    }

    maxTimeRead = 0;
    maxTimeWrite = 0;
  }


  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ dn, prop, ts, val, id }]
   */
  async function write(id, payload, table) {
    let columns;
    const beginTime = Date.now();
    const tableName = table || 'records';

    /*if (tableName == 'records' || tableName == 'strrecords') {
      //if (columnsCnt > 3) {
        columns = getColumns(tableName);
        payload.forEach(item => {
          item.tstz = `to_timestamp(${item.ts / 1000})`;
          item.q = 0;
        });
      } else {
        //columns = ['ts', 'id', 'val', 'q'];
      }
    } else {*/
    columns = getColumns(tableName);
    //}

    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;
    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ';
    const values1 = values.map(i => `(${i})`).join(', ');
    let sql = query + ' ' + values1;
    logger.log('Sql ' + sql, 1);
    try {
      await client.query(sql);
      const endTime = Date.now();
      if (maxTimeWrite < endTime - beginTime) {
        maxTimeWrite = endTime - beginTime;
        if (process.connected)
          process.send({
            type: 'procinfo',
            data: { lastMaxTimeWrite: maxTimeWrite, lastMaxCountWrite: payload.length }
          });
      }
      logger.log('Write query id=' + id + util.inspect(payload), 2);
    } catch (err) {
      sendError(id, err);
    }


  }

  async function writeCustom(id, payload, table) {
    logger.log('writeCustom' + util.inspect(payload), 1);
    const columns = getColumns(table);
    const query = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ';
    const values = payload.map(i => `('${i.name}', ${i.ts}, json('${i.payload}'))`).join(', ');
    const sql = query + ' ' + values;
    try {
      await client.run(sql);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function updateCustom(query, payload, table) {
    logger.log('updateCustom ' + util.inspect(payload), 1);
    if (query.sql) {
      try {
        await client.run(query.sql);
      } catch (err) {
        sendError('update', err);
      }
      return;
    }

    for (let j = 0; j < payload.length; j++) {
      const patch = payload[j].$set;
      // Формируем JSON-патч и экранируем для SQL
      const patchJson = JSON.stringify(patch).replace(/'/g, "''");

      const sql = `UPDATE ${table} 
                     SET payload = json_merge_patch(payload, '${patchJson}'::JSON)
                     WHERE ID = ${payload[j].id}`;

      logger.log('updateCustom ' + sql, 1);
      try {
        await client.run(sql);
      } catch (err) {
        sendError('update', err);
      }
    }
  }


  /**
   * update
   * @param {Object} query
   * @param {Array of Objects} payload
   *     [{id, $set:{<field>:<val>, <field_json>:{<field1>:<val1>,..}, <field_json_with_arr>:{<field1[idx]>:<val1>,..} }}]
   *
   *        Ex1: [{id, $set:{active:1, description:'xxx'}}]
   *        Ex2: [{id, $set:{jhead:{BatchWeight:3000, ...}}}]
   *        Ex3: [{id, $set:{jrows:{OpType[0]:1,OpType[5]:1 }}}]
   * @param {String} table
   */
  async function update(query, payload, table) {
    logger.log('update ' + util.inspect(payload), 1);

    if (query.sql) {
      try {
        await client.run(query.sql);
      } catch (err) {
        sendError('update', err);
      }
      return;
    }

    for (let j = 0; j < payload.length; j++) {
      const setObj = payload[j].$set || {};
      const fieldsArr = Object.keys(setObj);
      if (!fieldsArr.length) continue;

      const setParts = [];
      const indexedFields = {}; // { column: { 'OpType[3]': 1, ... } }

      for (const field of fieldsArr) {
        const fieldVal = setObj[field];

        if (fieldVal !== null && typeof fieldVal === 'object' && !Array.isArray(fieldVal)) {
          const keys = Object.keys(fieldVal);
          const hasIndexed = keys.some(k => k.indexOf('[') > 0);

          if (hasIndexed) {
            indexedFields[field] = fieldVal;
          } else {
            const patchJson = JSON.stringify(fieldVal).replace(/'/g, "''");
            setParts.push(
              `${field} = json_merge_patch(${field}, '${patchJson}'::JSON)`
            );
          }
        } else {
          setParts.push(getFieldSet(field, fieldVal));
        }
      }

      // read-modify-write для полей с индексами
      for (const [column, updates] of Object.entries(indexedFields)) {
        const current = await fetchJsonColumn(table, payload[j].id, column);
        applyIndexedUpdates(current, updates, column);
        const newJson = JSON.stringify(current).replace(/'/g, "''");
        setParts.push(`${column} = '${newJson}'::JSON`);
      }

      if (!setParts.length) continue;

      const sql = `UPDATE ${table} SET ${setParts.join(', ')} WHERE ID = ${payload[j].id}`;
      logger.log(sql, 1);

      try {
        await client.run(sql);
      } catch (err) {
        sendError('update', err);
      }
    }
  }

  // ---- helpers ----

  function getFieldSet(field, val) {
    if (val === null || val === undefined) return `${field} = NULL`;
    if (typeof val === 'number' || typeof val === 'boolean') return `${field} = ${val}`;
    if (typeof val === 'string') return `${field} = '${val.replace(/'/g, "''")}'`;
    const json = JSON.stringify(val).replace(/'/g, "''");
    return `${field} = '${json}'::JSON`;
  }

  async function fetchJsonColumn(table, id, column) {
    const res = await client.run(
      `SELECT ${column} AS v FROM ${table} WHERE ID = ${id}`
    );
    // @duckdb/node-api: res — ридер
    let row;
    if (res && typeof res.getRowObjects === 'function') {
      row = res.getRowObjects()[0];
    } else if (Array.isArray(res)) {
      row = res[0];
    } else if (res && res.rows) {
      row = res.rows[0];
    }

    let raw = row ? (row.v !== undefined ? row.v : row[column]) : null;
    if (raw == null) return {};
    if (typeof raw === 'string') {
      try { return JSON.parse(raw); } catch { return {}; }
    }
    return raw; // драйвер вернул уже объект
  }

  function applyIndexedUpdates(obj, updates, column) {
    for (const [key, val] of Object.entries(updates)) {
      const m = key.match(/^(.+)\[(\d+)\]$/);
      if (!m) {
        obj[key] = val;
        continue;
      }
      const [, prop, idxStr] = m;
      const idx = Number(idxStr);
      if (!Array.isArray(obj[prop])) obj[prop] = [];
      // Ваша логика: для jrows всегда числа
      obj[prop][idx] = (column === 'jrows') ? Number(val) : val;
    }
  }

  async function removeCustom(payload, table) {
    logger.log('removeCustom' + util.inspect(payload), 1);
    const values = payload.map(i => `${i.id}`).join(', ');
    const sql = `DELETE FROM ${table} WHERE id IN (${values})`;
    try {
      const changes = await client.query(sql);
      send({ id, payload: changes });
      logger.log(`Row(s) removed ${changes}`, 2);
    } catch (err) {
      sendError('remove', err);
    }
  }

  async function removeAll(table) {
    logger.log('removeall' + table, 1);
    const sql = `DELETE FROM ${table}`;
    try {
      const changes = await client.query(sql);
      logger.log(`All rows removed ${changes}`, 1);
      send({ id, payload: changes });
    } catch (err) {
      sendError('removeall', err);
    }
  }

  /*
  async function del(options) {
    let archDays = [1, 7, 15, 30, 90, 180, 360, 366, 500, 732, 1098];

    let tableName = 'records';
    for (const archDay of archDays) {
      let arrId = options.rp.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrId);
    }
  }
  */

  async function del(payload) {
    const { rp, rpstr } = payload;
    await delPointsForTable(rp, 'records');
    await delPointsForTable(rpstr, 'strrecords');
  }

  async function delPointsForTable(arr, tableName) {
    if (!arr || !arr.length) return;

    let archDays = [1, 7, 15, 30, 90, 180, 360, 366, 500, 732, 1098];
    for (const archDay of archDays) {
      const arrDnProp = arr.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrDnProp);
    }
  }

  async function deletePoints(tableName, archDay, arrId) {
    //if (compressedTablesSet.has(tableName)) return;

    if (!archDay || archDay <= 0) archDay = 1;
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrId), 1);
    let archDepth = archDay * 86400000;

    let delTime = Date.now() - archDepth;
    if (!arrId.length) return;
    while (arrId.length > 0) {
      let chunk = arrId.splice(0, 500);
      let values = chunk.map(i => `(id=${i.id})`).join(' OR ');
      logger.log('Map=' + values, 1);
      let sql = `DELETE FROM ${tableName} WHERE (${values}) AND ts<${delTime}`;
      logger.log('SQL: ' + sql, 1);
      try {
        const changes = await client.query(sql);
        logger.log(`Row(s) deleted ${changes}`, 1);
      } catch (err) {
        sendError('delete', err);
      }
    }
  }

  async function read(id, queryObj) {
    const beginTime = Date.now();
    let idarr;
    let dnarr;
    try {
      logger.log('queryObj: ' + util.inspect(queryObj), 2);
      let queryStr;
      if (queryObj.sql) {
        queryStr = queryObj.sql;
      } else {
        if (!queryObj.dn_prop) throw { message: 'Expected dn_prop in query ' };
        if (queryObj.table == 'timeline') {
          dnarr = queryObj.dn_prop.split(',');
          queryStr = utils.getQueryStrDn(queryObj, dnarr);
        } else {
          idarr = queryObj.ids.split(',');
          queryStr = utils.getQueryStrId(queryObj, idarr);
        }
      }
      logger.log('SQL: ' + queryStr, 2);
      let firstTime = Date.now();

      const result = await client.query(queryStr);
      //logger.log('Result: ' + util.inspect(result), 1);
      const endTime = Date.now();
      if (maxTimeRead < endTime - beginTime) {
        maxTimeRead = endTime - beginTime;
        if (process.connected)
          process.send({ type: 'procinfo', data: { lastMaxTimeRead: maxTimeRead, lastMaxCountRead: result.length } });
      }
      logger.log('Get result ' + id, 2);
      let payload = [];
      if (queryObj.sql || queryObj.table == 'timeline') {
        payload = result;
        payload.forEach(obj => {
          obj.ts = Number(obj.ts); // Унарный плюс - быстрый способ
        });
      } else {
        payload = queryObj.target == 'trend' ? formForTrend(result) : utils.recordsFor(result, queryObj, logger);
      }

      logger.log('payload ' + util.inspect(payload), 2);
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {

      // return idarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, idarr);
      return idarr.length == 1 ? res.map(item => [Number(item.ts), item.val]) : utils.recordsForTrend(res, idarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  // NEW
  async function run(id, queryObj) {
    try {
      if (!queryObj.sql) throw { message: 'Expect sql clause!' };
      const sql = queryObj.sql;
      logger.log('run:' + util.inspect(sql), 1);
      const payload = await client.run(sql);
      send({ id, payload });
    } catch (err) {
      sendError(id, err);
    }
  }
  function send(message) {
    if (channel.connected) channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    const res = {};
    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.end();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    console.log('processExit msg=' + msg);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }
};

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
        `id INTEGER PRIMARY KEY DEFAULT nextval('timeline_id_seq'),
        dn text NOT NULL, 
        prop text,
        start bigint NOT NULL,
        "end" bigint NOT NULL,
        state text`
      break;
    case 'customtable':
      result =
        `id INTEGER PRIMARY KEY DEFAULT nextval('customtable_id_seq'), 
        name text NOT NULL, 
        ts bigint NOT NULL, 
        payload json`
      break;
    case 'formulas':
      result =
        `id INTEGER PRIMARY KEY DEFAULT nextval('formulas_id_seq'),
        rid text NOT NULL,
        title text NOT NULL,
        description text,
        comments text,
        active integer,
        ts bigint,
        jhead json,
        jrows json`
      break;

    case 'recids':
      result =
        `id INTEGER PRIMARY KEY DEFAULT nextval('recids_id_seq'), 
        did text NOT NULL, 
        dn text NOT NULL, 
        prop text NOT NULL`
      break;

    case 'strrecords':
      result = 'ts bigint NOT NULL, id integer, val text, q integer';
      break;

    case 'staterecords':
      result = 'ts bigint NOT NULL, id integer, val integer, q integer';
      break;

    case 'records':
      result = 'ts bigint NOT NULL, id integer, val real, q integer';
      break;

    default:
      result = 'ts bigint NOT NULL, id integer, val real, q integer';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', '"end"', 'state'];
    case 'customtable':
      return ['name', 'ts', 'payload'];
    case 'formulas':
      return ['id', 'rid', 'title', 'active', 'ts', 'description', 'comments', 'jhead', 'jrows'];

    case 'recids':
      return ['id', 'did', 'dn', 'prop'];

    default:
      return ['ts', 'id', 'val', 'q'];
  }
}
