const { DuckDBInstance } = require('@duckdb/node-api');

(async () => {
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();

    await conn.run(`LOAD quack;`);


  const res = await conn.runAndReadAll(`
    CALL quack_serve('quack:localhost:9495', token = 'Intra_secret')
  `);
  console.log('Сервер запущен:', res.getRowObjects());

  await conn.run(`CREATE TABLE hello AS FROM VALUES ('world') v(s)`);

  // Держим процесс живым
  console.log('Сервер работает. Ctrl+C для остановки.');
  process.stdin.resume();
})();