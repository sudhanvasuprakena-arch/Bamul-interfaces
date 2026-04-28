#!/usr/bin/env node

/**
 * PO-AP Interface Scheduler
 *
 * Connects to the new Oracle EBS instance and runs:
 *   Process A  – XXCUST_PO_AP_INTERFACE_PKG.run_receipt_interface
 *   Process B  – XXCUST_PO_AP_INTERFACE_PKG.run_rtv_interface
 *   Purge      – XXCUST_PO_AP_INTERFACE_PKG.purge_log
 *
 * Configuration: config.json (copy config.json.example and fill in values)
 * Schedule:      cron (see crontab.example)
 *
 * Exit codes:  0 = success, 1 = warnings/rejections, 2 = fatal error
 */

'use strict';

const oracledb = require('oracledb');
const { Client } = require('ssh2');
const net = require('net');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Load configuration
// ---------------------------------------------------------------------------

const CONFIG_PATH = path.join(__dirname, 'config.json');

if (!fs.existsSync(CONFIG_PATH)) {
  console.error(
    'ERROR: config.json not found. Copy config.json.example to config.json and fill in your values.'
  );
  process.exit(2);
}

const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));

// Resolve active connection profile
const activeConnName = config.activeConnection || Object.keys(config.connections)[0];
const activeProfile = config.connections && config.connections[activeConnName];
if (!activeProfile) {
  console.error(
    `ERROR: Connection profile "${activeConnName}" not found in config.connections.`
  );
  process.exit(2);
}
config.ssh = activeProfile.ssh || { enabled: false };
config.connection = activeProfile.connection;

// Build fully-qualified package name (e.g. "apps.XXCUST_PO_AP_INTERFACE_PKG" or just "XXCUST_PO_AP_INTERFACE_PKG")
const PKG_SCHEMA = activeProfile.packageSchema ? activeProfile.packageSchema + '.' : '';
const PKG = `${PKG_SCHEMA}XXCUST_PO_AP_INTERFACE_PKG`;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

const LOG_DIR = path.resolve(__dirname, config.logging.logDir || './logs');
fs.mkdirSync(LOG_DIR, { recursive: true });

const RUN_TIMESTAMP = new Date()
  .toISOString()
  .replace(/[:\-T]/g, '')
  .slice(0, 14); // YYYYMMDDHHMMSS

const LOG_FILE = path.join(LOG_DIR, `po_ap_interface_${RUN_TIMESTAMP}.log`);
const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

function log(message) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const line = `${ts} | ${message}`;
  console.log(line);
  logStream.write(line + '\n');
}

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------

const MONTHS = [
  'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
  'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC',
];

/** Format a JS Date as DD-MON-YYYY (Oracle date literal format) */
function formatOracleDate(d) {
  const dd = String(d.getDate()).padStart(2, '0');
  const mon = MONTHS[d.getMonth()];
  const yy = d.getFullYear();
  return `${dd}-${mon}-${yy}`;
}

/** Return { dateFrom, dateTo } – from config (null stays null), or rolling yesterday→today if both are absent */
function resolveDateRange(procConfig) {
  if (procConfig.dateFrom != null && procConfig.dateTo != null) {
    return { dateFrom: procConfig.dateFrom, dateTo: procConfig.dateTo };
  }
  if (procConfig.dateFrom == null && procConfig.dateTo == null) {
    return { dateFrom: null, dateTo: null };
  }
  // One set and one null – fall back to rolling window
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  return {
    dateFrom: procConfig.dateFrom || formatOracleDate(yesterday),
    dateTo: procConfig.dateTo || formatOracleDate(today),
  };
}

// ---------------------------------------------------------------------------
// DBMS_OUTPUT capture
// ---------------------------------------------------------------------------

async function enableDbmsOutput(connection) {
  await connection.execute('BEGIN DBMS_OUTPUT.ENABLE(1000000); END;');
}

async function fetchDbmsOutput(connection) {
  const lines = [];
  let result;
  do {
    result = await connection.execute(
      `BEGIN DBMS_OUTPUT.GET_LINE(:line, :status); END;`,
      {
        line: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 32767 },
        status: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
      }
    );
    if (result.outBinds.status === 0) {
      lines.push(result.outBinds.line);
    }
  } while (result.outBinds.status === 0);
  return lines;
}

// ---------------------------------------------------------------------------
// Process A: Receipt-to-Invoice
// ---------------------------------------------------------------------------

async function runProcessA(connection) {
  const { dateFrom, dateTo } = resolveDateRange(config.processA);
  const procA = config.processA;

  log('============================================================');
  log('PROCESS A: Receipt-to-Invoice Interface');
  log(`  Operating Unit : ${procA.operatingUnit || 'ALL'}`);
  log(`  Date From      : ${dateFrom || 'NULL'}`);
  log(`  Date To        : ${dateTo || 'NULL'}`);
  log(`  PO Number      : ${procA.poNumber || 'ALL'}`);
  log(`  Debug Mode     : ${procA.debugMode || 'N'}`);
  log('============================================================');

  await enableDbmsOutput(connection);

  const result = await connection.execute(
    `BEGIN
       ${PKG}.run_receipt_interface(
         p_errbuf            => :errbuf,
         p_retcode           => :retcode,
         p_operating_unit    => :operating_unit,
         p_receipt_date_from => :date_from,
         p_receipt_date_to   => :date_to,
         p_po_number         => :po_number,
         p_debug_mode        => :debug_mode
       );
     END;`,
    {
      errbuf: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 2000 },
      retcode: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
      operating_unit: procA.operatingUnit || null,
      date_from: dateFrom,
      date_to: dateTo,
      po_number: procA.poNumber || null,
      debug_mode: procA.debugMode || 'N',
    }
  );

  const retcode = result.outBinds.retcode;
  const errbuf = result.outBinds.errbuf || '';

  // Capture DBMS_OUTPUT lines
  const dbmsLines = await fetchDbmsOutput(connection);
  for (const line of dbmsLines) {
    log(`  [DBMS_OUTPUT] ${line}`);
  }

  log(`Process A Return Code: ${retcode}`);
  if (errbuf) log(`Process A Message    : ${errbuf}`);

  return retcode;
}

// ---------------------------------------------------------------------------
// Process B: RTV-to-Credit-Memo
// ---------------------------------------------------------------------------

async function runProcessB(connection) {
  const { dateFrom, dateTo } = resolveDateRange(config.processB);
  const procB = config.processB;

  log('============================================================');
  log('PROCESS B: RTV-to-Credit-Memo Interface');
  log(`  Operating Unit : ${procB.operatingUnit || 'ALL'}`);
  log(`  Date From      : ${dateFrom || 'NULL'}`);
  log(`  Date To        : ${dateTo || 'NULL'}`);
  log(`  PO Number      : ${procB.poNumber || 'ALL'}`);
  log(`  Debug Mode     : ${procB.debugMode || 'N'}`);
  log('============================================================');

  await enableDbmsOutput(connection);

  const result = await connection.execute(
    `BEGIN
       ${PKG}.run_rtv_interface(
         p_errbuf            => :errbuf,
         p_retcode           => :retcode,
         p_operating_unit    => :operating_unit,
         p_rtv_date_from     => :date_from,
         p_rtv_date_to       => :date_to,
         p_po_number         => :po_number,
         p_debug_mode        => :debug_mode
       );
     END;`,
    {
      errbuf: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 2000 },
      retcode: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
      operating_unit: procB.operatingUnit || null,
      date_from: dateFrom,
      date_to: dateTo,
      po_number: procB.poNumber || null,
      debug_mode: procB.debugMode || 'N',
    }
  );

  const retcode = result.outBinds.retcode;
  const errbuf = result.outBinds.errbuf || '';

  const dbmsLines = await fetchDbmsOutput(connection);
  for (const line of dbmsLines) {
    log(`  [DBMS_OUTPUT] ${line}`);
  }

  log(`Process B Return Code: ${retcode}`);
  if (errbuf) log(`Process B Message    : ${errbuf}`);

  return retcode;
}

// ---------------------------------------------------------------------------
// Purge old log records
// ---------------------------------------------------------------------------

async function runPurge(connection) {
  const days = config.purge.daysToKeep || 90;
  log(`Purging processed log records older than ${days} days...`);

  await enableDbmsOutput(connection);

  await connection.execute(
    `BEGIN ${PKG}.purge_log(p_days_to_keep => :days); END;`,
    { days }
  );

  const dbmsLines = await fetchDbmsOutput(connection);
  for (const line of dbmsLines) {
    log(`  [DBMS_OUTPUT] ${line}`);
  }
}

// ---------------------------------------------------------------------------
// Rotate old log files
// ---------------------------------------------------------------------------

function rotateLogFiles() {
  const retentionDays = config.logging.retentionDays || 30;
  const cutoff = Date.now() - retentionDays * 24 * 60 * 60 * 1000;

  let deleted = 0;
  for (const file of fs.readdirSync(LOG_DIR)) {
    const filePath = path.join(LOG_DIR, file);
    try {
      const stat = fs.statSync(filePath);
      if (stat.isFile() && stat.mtimeMs < cutoff) {
        fs.unlinkSync(filePath);
        deleted++;
      }
    } catch {
      // skip files we can't stat
    }
  }
  if (deleted > 0) {
    log(`Log rotation: deleted ${deleted} log file(s) older than ${retentionDays} days.`);
  }
}

// ---------------------------------------------------------------------------
// SSH Tunnel
// ---------------------------------------------------------------------------

/**
 * Opens an SSH tunnel that forwards a local port to remoteHost:remotePort.
 * Returns { localPort, sshClient, server } for cleanup.
 */
function openSshTunnel(sshConfig, remoteHost, remotePort) {
  return new Promise((resolve, reject) => {
    const keyPath = path.resolve(__dirname, sshConfig.privateKeyPath);
    if (!fs.existsSync(keyPath)) {
      return reject(new Error(`SSH private key not found: ${keyPath}`));
    }

    const sshClient = new Client();
    let sshReady = false;

    const server = net.createServer((sock) => {
      sshClient.forwardOut(
        sock.remoteAddress || '127.0.0.1',
        sock.remotePort || 0,
        remoteHost,
        remotePort,
        (err, stream) => {
          if (err) {
            sock.end();
            return;
          }
          sock.pipe(stream).pipe(sock);
        }
      );
    });

    server.on('error', reject);

    sshClient
      .on('ready', () => {
        sshReady = true;
        // SSH is ready — now start the local TCP server
        server.listen(0, '127.0.0.1', () => {
          const localPort = server.address().port;
          resolve({ localPort, sshClient, server });
        });
      })
      .on('error', (err) => {
        server.close();
        reject(err);
      })
      .connect({
        host: sshConfig.host,
        port: sshConfig.port || 22,
        username: sshConfig.username,
        privateKey: fs.readFileSync(keyPath),
      });
  });
}

function closeSshTunnel(tunnel) {
  if (!tunnel) return;
  try { tunnel.server.close(); } catch { /* ignore */ }
  try { tunnel.sshClient.end(); } catch { /* ignore */ }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  let connection;
  let tunnel;
  let exitCode = 0;

  try {
    // Initialize Oracle Client (Thick mode — required for NNE/ANO encryption)
    try {
      oracledb.initOracleClient({ libDir: config.connection.oracleClientPath });
    } catch (err) {
      if (!err.message.includes('already initialized')) {
        throw err;
      }
    }

    log('============================================================');
    log('PO-AP Interface Scheduler - Starting');
    log(`  Timestamp  : ${new Date().toISOString()}`);
    log(`  Connection : ${activeConnName}`);
    log(`  Log File   : ${LOG_FILE}`);
    log('============================================================');

    // ---- SSH Tunnel (optional) ----
    let connectString = config.connection.connectString;

    if (config.ssh && config.ssh.enabled) {
      // Parse remote Oracle host:port from connectString (e.g. "localhost:1521/EBSDB")
      const match = connectString.match(/^([^:]+):(\d+)(\/.*)?$/);
      const remoteHost = match ? match[1] : 'localhost';
      const remotePort = match ? parseInt(match[2], 10) : 1521;
      const servicePart = match && match[3] ? match[3] : '';

      log(`Opening SSH tunnel to ${config.ssh.host} -> ${remoteHost}:${remotePort} ...`);
      tunnel = await openSshTunnel(config.ssh, remoteHost, remotePort);
      connectString = `127.0.0.1:${tunnel.localPort}${servicePart}`;
      log(`SSH tunnel established (local port ${tunnel.localPort}).`);
    }

    // Connect to Oracle
    connection = await oracledb.getConnection({
      user: config.connection.user,
      password: config.connection.password,
      connectString: connectString,
    });
    log('Connected to Oracle database.');

    // ---- Process A ----
    let retcodeA = 0;
    if (config.processA.enabled) {
      retcodeA = await runProcessA(connection);
      if (retcodeA === 2) {
        log('FATAL: Process A returned retcode 2. Skipping Process B.');
        exitCode = 2;
      } else if (retcodeA === 1) {
        exitCode = Math.max(exitCode, 1);
      }
    } else {
      log('Process A: DISABLED (config.processA.enabled = false). Skipping.');
    }

    // ---- Process B (only if A did not fatally fail) ----
    if (retcodeA !== 2) {
      if (config.processB.enabled) {
        const retcodeB = await runProcessB(connection);
        if (retcodeB === 2) {
          exitCode = 2;
        } else if (retcodeB === 1) {
          exitCode = Math.max(exitCode, 1);
        }
      } else {
        log('Process B: DISABLED (config.processB.enabled = false). Skipping.');
      }
    }

    // ---- Purge ----
    if (config.purge && config.purge.enabled) {
      await runPurge(connection);
    }

  } catch (err) {
    log(`FATAL ERROR: ${err.message}`);
    if (err.stack) log(err.stack);
    exitCode = 2;
  } finally {
    if (connection) {
      try {
        await connection.close();
        log('Oracle connection closed.');
      } catch (err) {
        log(`Error closing connection: ${err.message}`);
      }
    }

    closeSshTunnel(tunnel);

    // Rotate old log files
    try {
      rotateLogFiles();
    } catch (err) {
      log(`Log rotation error: ${err.message}`);
    }

    log('============================================================');
    log(`PO-AP Interface Scheduler - Finished (exit code: ${exitCode})`);
    log('============================================================');
    logStream.end();
  }

  process.exit(exitCode);
}

main();
