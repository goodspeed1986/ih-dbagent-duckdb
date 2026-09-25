const { DuckDBInstance } = require('@duckdb/node-api');

async function main() {
    // База в памяти
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();

    try {
        console.log('=== Версия DuckDB ===');
        const versionReader = await connection.runAndReadAll('SELECT version() AS v');
        console.table(versionReader.getRowObjects());

        console.log('\n=== Все JSON-функции ===');
        const reader = await connection.runAndReadAll(`
            SELECT 
                function_name,
                function_type,
                return_type,
                parameters
            FROM duckdb_functions()
            WHERE function_name LIKE 'json%'
            ORDER BY function_name
        `);
        const rows = reader.getRowObjects();
        console.table(rows);

        console.log('\n=== Проверка конкретных функций ===');
        const interesting = [
            'json_set',
            'json_replace',
            'json_merge_patch',
            'json_extract',
            'json_extract_string',
            'json_transform',
            'json_array_length',
            'json_keys',
            'json_contains',
            'json_type'
        ];

        const available = new Set(rows.map(r => r.function_name));
        for (const fn of interesting) {
            console.log(`${available.has(fn) ? '✅' : '❌'} ${fn}`);
        }

    } catch (err) {
        console.error('Ошибка:', err);
    } finally {
        connection.closeSync();
        instance.closeSync();
    }
}

main();