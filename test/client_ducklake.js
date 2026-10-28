const { DuckDBInstance } = require("@duckdb/node-api");

async function main() {
  const token =
    process.env.QUACK_TOKEN || "replace-with-a-long-random-token";

  const instance = await DuckDBInstance.create(":memory:");
  const conn = await instance.connect();

  let stopped = false;
  let reading = false;
  let timer;

  try {
    await conn.run("INSTALL quack");
    await conn.run("LOAD quack");

    console.log("[client] connected (quack_query mode)");

    async function readLatest() {
      if (reading || stopped) return;
      reading = true;

      try {
        const since = Date.now() - 10_000;

        // Оставляем ts как TIMESTAMP, без epoch_ms()
        const innerSql = `
          SELECT ts, id, val, q
          FROM lake.main.records
          WHERE ts >= make_timestamp_ms(${since})
          ORDER BY ts DESC
          LIMIT 10
        `.replace(/'/g, "''");

        const result = await conn.runAndReadAll(`
          SELECT * FROM quack_query(
            'quack:localhost:9494',
            '${innerSql}',
            token = '${token.replace(/'/g, "''")}'
          )
        `);

        const rows = result.getRowObjects().map(row => ({
          ts: timestampToISO(row.ts),   // читаемый вид
          id: Number(row.id),
          val: Number(row.val),
          q: Number(row.q)
        }));

        console.log("[client]", rows);
      } catch (err) {
        console.error("[client] read error:", err.message);
      } finally {
        reading = false;
      }
    }

    await readLatest();
    timer = setInterval(readLatest, 3000);

    process.once("SIGINT", () => {
      stopped = true;
      clearInterval(timer);
      console.log("[client] stopping...");
      conn.closeSync();
      instance.closeSync();
      process.exit(0);
    });

    process.stdin.resume();
  } catch (err) {
    conn.closeSync();
    instance.closeSync();
    throw err;
  }
}

// DuckDBTimestampValue { micros: bigint } → ISO-строка
function timestampToISO(v) {
  if (v == null) return null;
  if (typeof v === "object" && typeof v.micros === "bigint") {
    return new Date(Number(v.micros / 1000n)).toISOString();
  }
  return String(v);
}

main().catch(err => {
  console.error("[client] fatal error:", err);
  process.exitCode = 1;
});