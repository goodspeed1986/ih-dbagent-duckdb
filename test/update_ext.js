const { DuckDBInstance } = require('@duckdb/node-api');

async function reinstallExtensions(extensions) {
  const instance = await DuckDBInstance.create();
  const connection = await instance.connect();
  await connection.run(`SET extension_directory = './extensions';`);
  try {
    for (const ext of extensions) {
      await connection.run(`FORCE INSTALL ${ext}`);
      console.log(`${ext} переустановлено`);
    }
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
}

// Переустановите все нужные расширения
reinstallExtensions(['json', 'quack']);