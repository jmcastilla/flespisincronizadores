require("dotenv").config();

const mqtt = require("mqtt");
const mysql = require("mysql2/promise");

// ===== ENV =====
const FLESPI_TOKEN = process.env.FLESPI_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;

const MYSQL_HOST = process.env.MYSQL_HOST;
const MYSQL_PORT = Number(process.env.MYSQL_PORT || 3306);
const MYSQL_USER = process.env.MYSQL_USER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || "";
const MYSQL_DATABASE = process.env.MYSQL_DATABASE;

if (!FLESPI_TOKEN) throw new Error("Falta FLESPI_TOKEN");
if (!CHANNEL_ID) throw new Error("Falta CHANNEL_ID");
if (!MYSQL_HOST || !MYSQL_USER || !MYSQL_DATABASE) {
  throw new Error("Faltan variables MySQL (MYSQL_HOST, MYSQL_USER, MYSQL_DATABASE)");
}

// ===== MQTT =====
const MQTT_URL = "mqtts://mqtt.flespi.io:8883";
const TOPIC = `flespi/message/gw/channels/${CHANNEL_ID}/+`;

// ===== Helpers =====
function b2i(v) {
  if (v === true) return 1;
  if (v === false) return 0;
  if (v === null || v === undefined) return undefined; // <- importante: undefined = "no incluir"
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function num(v) {
  if (v === null || v === undefined) return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

function str(v) {
  if (v === null || v === undefined) return undefined;
  const s = String(v);
  return s.length ? s : undefined;
}

function serverTimestampToMs(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return undefined;
  return Math.round(n * 1000);
}

function normalizeToArray(parsed) {
  if (Array.isArray(parsed)) return parsed;
  if (parsed && typeof parsed === "object") return [parsed];
  return [];
}

/**
 * Mapa JSON key -> columna MySQL (solo las que realmente existen en tu tabla)
 * OJO: removí eventnum porque tu tabla NO la tiene.
 */
const KEY_TO_COL = {
  "ident": "deviceID",
  "protocol.version": "protocolVersion",
  "timestamp": "dateTime", // solo si tu json trae esto y tu tabla lo tiene
  "position.latitude": "latitude",
  "position.longitude": "longitude",
  "engine.ignition.status": "ignition_status",
  "position.speed": "speed",
  "vehicle.mileage": "mileage",
  "gsm.mcc": "mcc",
  "gsm.cellid": "cellID",
  "gsm.lac": "lacID",
  "position.altitude": "altitude"
};

/**
 * Convertidores por key (para tipos correctos)
 * Si no hay convertidor, se inserta como viene.
 */
const CONVERTERS = {
  "engine.ignition.status": b2i,
  "ident": str,
  "protocol.version": str,
  "position.altitude": num,
  "position.latitude": num,
  "position.longitude": num,
  "position.speed": num,
  "timestamp": unixSecondsToMySQLDateTime,
  "vehicle.mileage": num,
  "gsm.mcc": num,
  "gsm.cellid": num,
  "gsm.lac": num
};

// Construye row SOLO con campos presentes en el JSON
function buildRowFromPayload(p) {
  const row = {};

  for (const [jsonKey, col] of Object.entries(KEY_TO_COL)) {
    if (!(jsonKey in p)) continue; // <- si no viene, lo omitimos

    const conv = CONVERTERS[jsonKey];
    const value = conv ? conv(p[jsonKey]) : p[jsonKey];

    if (value === undefined) continue; // <- si no es válido, omitimos
    row[col] = value;
  }
  row.OperadorGps = "QL-GV300";
  row.imei = row.deviceID;
  row.bat = 100;
  row.battery = 100;
  row.lock_status = 1;
  row.insertDateTime = nowUtcMySQLDateTime();
  row.mileage=0;
  return row;
}

function nowUtcMySQLDateTime() {
  const d = new Date();
  const pad = (x) => String(x).padStart(2, "0");

  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
         `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

function unixSecondsToMySQLDateTime(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n)) return undefined;

  // Si alguna vez te llega en milisegundos, lo detecta
  const ms = n > 1e12 ? n : n * 1000;
  const d = new Date(ms);

  const pad = (x) => String(x).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}


// Carga columnas reales de la tabla, para evitar "Unknown column"
async function loadTableColumns(db) {
  const [rows] = await db.execute(
    `SELECT COLUMN_NAME AS col
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
    [MYSQL_DATABASE, "mainData"]
  );

  const set = new Set(rows.map(r => r.col));
  // idmensaje existe pero NO la insertamos nunca
  return set;
}

async function insertDynamic(db, tableColsSet, row) {
  console.log("entro 3");
  // Filtra por columnas que existen en la tabla
  const cols = Object.keys(row).filter(c => tableColsSet.has(c));

  // Si no hay nada que insertar, no hacemos INSERT
  if (cols.length === 0) return;

  const vals = cols.map(c => row[c]);
  const placeholders = cols.map(() => "?").join(",");

  const sql = `INSERT INTO mainData (${cols.join(",")}) VALUES (${placeholders})`;
  console.log(sql);
  await db.execute(sql, vals);
}

// ===== Main =====
async function main() {
  const db = await mysql.createPool({
    host: MYSQL_HOST,
    port: MYSQL_PORT,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  });

  // 1) Leer columnas reales de x200 (evita el error de columnas inexistentes)
  const tableColsSet = await loadTableColumns(db);
  console.log("✅ Columnas cargadas de mainData:", tableColsSet.size);

  const http = require("http");
http.createServer((req, res) => {
  res.writeHead(200, {"Content-Type": "text/plain"});
  res.end("OK");
}).listen(process.env.PORT || 3000);

  const client = mqtt.connect(MQTT_URL, {
    username: FLESPI_TOKEN,
    password: "",
    keepalive: 60,
    reconnectPeriod: 2000,
    clean: true,
  });

  client.on("connect", () => {
    console.log("✅ Conectado a flespi MQTT");
    client.subscribe(TOPIC, { qos: 0 }, (err) => {
      if (err) console.error("❌ Error subscribe:", err);
      else console.log("📡 Suscrito a:", TOPIC);
    });
  });

  client.on("message", async (_topic, payloadBuf) => {
    const rawStr = payloadBuf.toString("utf8");

    let parsed;
    try {
      parsed = JSON.parse(rawStr);
    } catch {
      console.error("❌ Payload no es JSON:", rawStr.slice(0, 200));
      return;
    }

    const items = normalizeToArray(parsed);
    if (!items.length) return;

    for (const p of items) {
      try {
        // 2) Construir row solo con campos presentes
        const row = buildRowFromPayload(p);
        console.log(p);
        console.log("entrooo 0");
        console.log(row);
        // Recomendado: exigir ident para no llenar basura
        if (!row.deviceID) continue;
        console.log("entrooo 1");
        // 3) Insert dinámico (solo columnas existentes)
        await insertDynamic(db, tableColsSet, row);
      } catch (e) {
        console.error("❌ Error insertando x200:", e);
      }
    }
  });

  client.on("error", (err) => console.error("MQTT error:", err));
  client.on("close", () => console.log("🔌 MQTT desconectado"));

  process.on("SIGINT", async () => {
    console.log("\n🛑 Cerrando...");
    try {
      client.end(true);
      await db.end();
    } catch {}
    process.exit(0);
  });
}

main().catch((e) => {
  console.error("❌ Fatal:", e);
  process.exit(1);
});
