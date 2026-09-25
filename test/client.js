const { DuckDBInstance } = require('@duckdb/node-api');

(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();
  await conn.run(`LOAD quack;`);
  await conn.run(`CALL enable_logging('Quack');`);

try {
  await conn.run(`
    ATTACH 'quack:localhost:9495' AS remote (
      TOKEN 'Intra_secret'
    )
  `);
  console.log('ATTACH OK');
} catch (e) {
  console.error('ATTACH failed:', e.message);
}

const ddl = await conn.runAndReadAll(`
  FROM quack_query(
    'quack:localhost:9495',
    $$
      SELECT schema_name, sql, 'table' AS kind
      FROM duckdb_tables()
      UNION ALL
      SELECT schema_name, view_name, 'view'
      FROM duckdb_views()
    $$,
    token = 'Intra_secret'
  )
`);
console.log(ddl.getRowObjects());



  // Читаем данные
  const data = await conn.runAndReadAll(`SELECT * FROM remote.hello`);
  console.log('Данные с сервера:', data.getRowObjects());

  // Можно писать на сервер
  await conn.run(`INSERT INTO remote.hello VALUES ('from client')`);
  const data2 = await conn.runAndReadAll(`SELECT * FROM remote.hello`);
  console.log('После INSERT:', data2.getRowObjects());

  // Список каталогов
  const dbs = await conn.runAndReadAll(`
    SELECT database_name, type FROM duckdb_databases()
  `);
  console.log('Каталоги:', dbs.getRowObjects());

  await conn.closeSync();
  await instance.closeSync();
})();