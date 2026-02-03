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
  "battery.level": "batterylevel",
  "battery.voltage": "batteryvoltage",
  "case.status": "casestatus", // solo si tu json trae esto y tu tabla lo tiene
  "channel.id": "channelid",
  "device.temperature": "devicetemperature",
  "engine.ignition.status": "engineignitionstatus",
  "event.enum": "eventenum",
  "event.seqnum": "eventseqnum",
  "external.powersource.status": "externalpowersourcestatus",
  "gnss.status": "gnssstatus",
  "gsm.signal.level": "gsmsignallevel",
  "ident": "ident",
  "lock.state": "lockstate",
  "message.buffered.status": "messagebufferedstatus",
  "message.type": "messagetype",
  "movement.status": "movementstatus",
  "peer": "peer",
  "position.altitude": "positionaltitude",
  "position.direction": "positiondirection",
  "position.latitude": "positionlatitude",
  "position.longitude": "positionlongitude",
  "position.satellites": "positionsatellites",
  "position.speed": "positionspeed",
  "position.valid": "positionvalid",
  "protocol.id": "protocolid",
  "report.reason": "reportreason",
  "server.timestamp": "servertimestamp",
  "solar.panel.charging.status": "solarpanelchargingstatus",
  "solar.panel.voltage": "solarpanelvoltage",
  "timestamp": "timestamp",
  "vehicle.mileage": "vehiclemileage",
  "x.acceleration": "xacceleration",
  "y.acceleration": "yacceleration",
  "z.acceleration": "zacceleration",
};

/**
 * Convertidores por key (para tipos correctos)
 * Si no hay convertidor, se inserta como viene.
 */
const CONVERTERS = {
  "battery.level": num,
  "battery.voltage": num,
  "case.status": num,
  "channel.id": num,
  "device.temperature": num,
  "engine.ignition.status": b2i,
  "event.enum": num,
  "event.seqnum": num,
  "external.powersource.status": b2i,
  "gnss.status": b2i,
  "gsm.signal.level": num,
  "ident": str,
  "lock.state": num,
  "message.buffered.status": b2i,
  "message.type": str,
  "movement.status": b2i,
  "peer": str,
  "position.altitude": num,
  "position.direction": num,
  "position.latitude": num,
  "position.longitude": num,
  "position.satellites": num,
  "position.speed": num,
  "position.valid": b2i,
  "protocol.id": num,
  "report.reason": num,
  "server.timestamp": serverTimestampToMs, // ✅ ms
  "solar.panel.charging.status": b2i,
  "solar.panel.voltage": num,
  "timestamp": num,
  "vehicle.mileage": num,
  "x.acceleration": num,
  "y.acceleration": num,
  "z.acceleration": num,
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

  return row;
}

// Carga columnas reales de la tabla, para evitar "Unknown column"
async function loadTableColumns(db) {
  const [rows] = await db.execute(
    `SELECT COLUMN_NAME AS col
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
    [MYSQL_DATABASE, "x200"]
  );

  const set = new Set(rows.map(r => r.col));
  // idmensaje existe pero NO la insertamos nunca
  return set;
}

async function insertDynamic(db, tableColsSet, row) {
  // Filtra por columnas que existen en la tabla
  const cols = Object.keys(row).filter(c => tableColsSet.has(c));

  // Si no hay nada que insertar, no hacemos INSERT
  if (cols.length === 0) return;

  const vals = cols.map(c => row[c]);
  const placeholders = cols.map(() => "?").join(",");

  const sql = `INSERT INTO x200 (${cols.join(",")}) VALUES (${placeholders})`;
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
  console.log("✅ Columnas cargadas de x200:", tableColsSet.size);

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
        // Recomendado: exigir ident para no llenar basura
        if (!row.ident) continue;

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
