const { DuckDBInstance } = require('@duckdb/node-api');
const util = require('util')
async function main() {
  const db = await DuckDBInstance.create('my_duckdb.db', {
  threads: '10'
})           // или 'data.duckdb' для файла
  const conn = await db.connect();               // или db.run без connect в простых случаях

  try {
    await conn.run(`
      CREATE TABLE IF NOT EXISTS records (
        id INTEGER NOT NULL, 
        val Real, 
        ts Bigint, 
        q INTEGER 
      )
    `);
    console.log('Таблица создана');
    await conn.run('CREATE INDEX IF NOT EXISTS idx_records_ds ON records (id, ts);');
    console.log('Индекс создан');


    const stmt = await conn.prepare(`
          INSERT INTO records (ts, id, val, q)
          VALUES (?, ?, ?, ?)
        `);

     await stmt.bind([
        1739808000,    // bigint → лучше BigInt(row.ts) если приходит number
        1005,
        52.75,
        0
     ])
     await stmt.run();
    // Пример вставки
   /* await conn.run(
      'INSERT INTO records (id, ts, val, q) VALUES (?, ?, ?, ?)',
      [1005, 1739808000, 42.75, 0]
    );*/

    
    //const rows = await conn.run('SELECT * FROM duckdb_extensions();')
    const selectResult = await conn.run('SELECT * FROM records');
    
    // Способ 1: Получить все строки как массив объектов (рекомендуется)
    const rows = await selectResult.getRows();
    console.log('Данные:', rows);
  } catch (err) {
    console.error('Ошибка:', err);
  } finally {
    conn.closeSync();
  }
}

main().catch(console.error);