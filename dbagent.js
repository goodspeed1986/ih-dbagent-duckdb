/**
 * dbagent.js
 * Точка входа при запуске дочернего процесса
 * Входной параметр - путь к файлу конфигурации или сама конфигурация как строка JSON
 *
 */

const path = require('path');
const dbagent_main = require('./lib/index');
const logger = require('./logger');
const utils = require('./lib/utils');

try {
  let opt;
  try {
    opt = JSON.parse(process.argv[2]); // dbPath property
    // opt.dbLimit = 1024;
  } catch (e) {
    opt = {};
  }

  const logfile = opt.logfile || path.join(__dirname, 'ih_duckdb.log');
  let loglevel = !opt.loglevel || isNaN(opt.loglevel) ? 0 : Number(opt.loglevel);

  logger.start(logfile, loglevel);

  // console.log('Start dbagent postgreSql. Options: ' + JSON.stringify(opt));
  logger.log('Start dbagent Duckdb. Options: ' + JSON.stringify(opt));



  delete opt.logfile;
  delete opt.loglevel;

  utils.sendProcessInfo();
  setInterval(utils.sendProcessInfo, 10000);
  dbagent_main(process, opt, logger);
} catch (e) {
  process.send({ error: utils.getShortErrStr(e) });
  setTimeout(() => {
    process.exit(99);
  }, 500);
}
