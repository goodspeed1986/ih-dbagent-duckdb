
const { DuckDBInstance } = require("@duckdb/node-api");
const path = require("node:path");
const fs = require("node:fs");

async function main() {
    const base = path.join(__dirname, "data");
    const catalogPath = path.join(base, "catalog.duckdb");
    const dataPath = path.join(base, "files") + path.sep;

    const TOKEN =
        process.env.QUACK_TOKEN || "replace-with-a-long-random-token";

    const BATCH_SIZE = 2500;
    const INTERVAL_MS = 1000;
    const FLUSH_INTERVAL_MS = 60_000;

    fs.mkdirSync(dataPath, { recursive: true });

    // Важно: каталог DuckLake не открываем напрямую как базу экземпляра.
    const instance = await DuckDBInstance.create(":memory:");
    const conn = await instance.connect();

    let counter = 0;
    let totalRows = 0;
    let batches = 0;
    let lastFlush = Date.now();
    let stopped = false;
    let loopPromise;

    process.once("SIGINT", () => {
        stopped = true;
        console.log("\nStopping...");
    });

    process.once("SIGTERM", () => {
        stopped = true;
        console.log("\nStopping...");
    });

    try {
        await conn.run("INSTALL ducklake");
        await conn.run("LOAD ducklake");

        await conn.run("INSTALL quack");
        await conn.run("LOAD quack");

        // DuckDB-файл используется только как каталог метаданных DuckLake.
        const catalogSqlPath = catalogPath.replace(/\\/g, "/");
        const dataSqlPath = dataPath.replace(/\\/g, "/");

        await conn.run(`
      ATTACH 'ducklake:${catalogSqlPath}' AS lake
      (
        DATA_PATH '${dataSqlPath}',
        DATA_INLINING_ROW_LIMIT 2500
      )
    `);
        await conn.run(`
        CALL lake.set_option('target_file_size', '128MB')
    `);
        await conn.run(`
      CREATE TABLE IF NOT EXISTS lake.main.records (
        ts TIMESTAMP NOT NULL,
        id BIGINT,
        val DOUBLE,
        q INTEGER
      )
      PARTITIONED BY (year(ts), month(ts))
      SORTED BY (ts)
    `);

        console.log("[server] DuckLake initialized");
        console.log("[server] Catalog:", catalogPath);
        console.log("[server] Parquet:", dataPath);

        // Открываем доступ другим DuckDB-процессам.
        await conn.run(`
      CALL quack_serve(
        'quack:localhost:9494',
        token = '${TOKEN.replace(/'/g, "''")}'
      )
    `);

        console.log("[server] Quack listening at quack:localhost:9494");

        async function writeBatch() {
            const started = Date.now();
            const values = [];
            const batchStartMs = started;

            for (let i = 0; i < BATCH_SIZE; i++) {
                const tsMs =
                    batchStartMs + Math.floor((i * 1000) / BATCH_SIZE);

                const id = counter++;
                const val = Math.random() * 100;
                const q = 0;

                values.push(`(${tsMs}, ${id}, ${val}, ${q})`);
            }

            await conn.run(`
        INSERT INTO lake.main.records (ts, id, val, q)
        SELECT
          make_timestamp_ms(ts_ms),
          id,
          val,
          q
        FROM (VALUES ${values.join(",")})
          AS v(ts_ms, id, val, q)
      `);

            batches++;
            totalRows += BATCH_SIZE;

            const elapsed = Date.now() - started;

            console.log(
                `[server] batch=${batches}, rows=${BATCH_SIZE}, ` +
                `elapsed=${elapsed}ms, total=${totalRows}`
            );

            if (Date.now() - lastFlush >= FLUSH_INTERVAL_MS) {
                const flushed = await conn.runAndReadAll(`
                    CALL ducklake_flush_inlined_data('lake')
                    `);

                // Сбрасываем счётчик только после успешного flush.
                //pendingRows = 0;
                console.log("[maintenance] flush:", flushed.getRowObjects());

                // 2. Объединяем мелкие Parquet-файлы.
                const merged = await conn.runAndReadAll(`
                    CALL ducklake_merge_adjacent_files(
                        'lake',
                        max_compacted_files => 100
                    )
                    `);

                lastFlush = Date.now();
                console.log("[server] inline data flushed to Parquet");
            }
        }

        // Последовательная запись: следующий батч не начнётся,
        // пока предыдущий INSERT не завершится.
        loopPromise = (async () => {
            while (!stopped) {
                const cycleStart = Date.now();

                try {
                    await writeBatch();
                } catch (err) {
                    console.error("[server] insert error:", err.message);
                }

                const remaining = INTERVAL_MS - (Date.now() - cycleStart);

                if (!stopped && remaining > 0) {
                    await new Promise(resolve => setTimeout(resolve, remaining));
                } else if (remaining < 0) {
                    console.warn(
                        `[server] batch exceeded interval by ${-remaining}ms`
                    );
                }
            }
        })();

        await loopPromise;
    } finally {
        stopped = true;

        if (loopPromise) {
            await loopPromise.catch(() => { });
        }

        try {
            await conn.run(`
        CALL ducklake_flush_inlined_data('lake')
      `);
        } catch (err) {
            console.error("[server] final flush failed:", err.message);
        }

        try {
            await conn.run(`CALL quack_stop('quack:localhost')`);
        } catch (err) {
            console.error("[server] Quack shutdown:", err.message);
        }

        conn.closeSync();
        instance.closeSync();

        console.log(`[server] stopped; total rows=${totalRows}`);
    }
}

main().catch(err => {
    console.error("[server] fatal error:", err);
    process.exitCode = 1;
});
