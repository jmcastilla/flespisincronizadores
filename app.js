/**
 * Node.js: Flespi MQTT -> Azure Event Hubs
 * - Lee mensajes MQTT de flespi
 * - Normaliza/convierte campos (igual que tu lógica)
 * - Publica cada registro a Azure Event Hubs (en batch)
 *
 * Reqs:
 *   npm i mqtt dotenv @azure/event-hubs
 *
 * .env:
 *   FLESPI_TOKEN=...
 *   CHANNEL_ID=...
 *   EVENTHUB_CONNECTION_STRING=Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=...
 *   EVENTHUB_NAME=gps-telemetry
 *   PORT=3000
 */

require("dotenv").config();

const mqtt = require("mqtt");
const http = require("http");
const { EventHubProducerClient } = require("@azure/event-hubs");

// ===== ENV =====
const FLESPI_TOKEN = process.env.FLESPI_TOKEN;
const CHANNEL_ID = process.env.CHANNEL_ID;

const EVENTHUB_CONNECTION_STRING = process.env.EVENTHUB_CONNECTION_STRING;
const EVENTHUB_NAME = process.env.EVENTHUB_NAME;

if (!FLESPI_TOKEN) throw new Error("Falta FLESPI_TOKEN");
if (!CHANNEL_ID) throw new Error("Falta CHANNEL_ID");
if (!EVENTHUB_CONNECTION_STRING) throw new Error("Falta EVENTHUB_CONNECTION_STRING");
if (!EVENTHUB_NAME) throw new Error("Falta EVENTHUB_NAME");

// ===== MQTT =====
const MQTT_URL = "mqtts://mqtt.flespi.io:8883";
const TOPIC = `flespi/message/gw/channels/${CHANNEL_ID}/+`;

// ===== Helpers =====
function b2i(v) {
  if (v === true) return 1;
  if (v === false) return 0;
  if (v === null || v === undefined) return undefined;
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

function normalizeToArray(parsed) {
  if (Array.isArray(parsed)) return parsed;
  if (parsed && typeof parsed === "object") return [parsed];
  return [];
}

function nowUtcMySQLDateTime() {
  const d = new Date();
  const pad = (x) => String(x).padStart(2, "0");
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`
  );
}

// Te recomiendo mandar ISO UTC (mejor para SQL Server) + opcional string tipo MySQL si lo necesitas
function unixSecondsToIsoUtc(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n)) return undefined;
  const ms = n > 1e12 ? n : n * 1000; // detecta ms vs s
  return new Date(ms).toISOString();
}

// (Opcional) si quieres mantener el formato "YYYY-MM-DD HH:mm:ss"
function unixSecondsToMySQLDateTime(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n)) return undefined;
  const ms = n > 1e12 ? n : n * 1000;
  const d = new Date(ms);
  const pad = (x) => String(x).padStart(2, "0");
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
  );
}

/**
 * Mapa JSON key -> columna (lo que tú quieres guardar)
 */
const KEY_TO_COL = {
  ident: "deviceID",
  "protocol.version": "protocolVersion",
  timestamp: "dateTime", // lo convertimos a ISO UTC (recomendado)
  "position.latitude": "latitude",
  "position.longitude": "longitude",
  "engine.ignition.status": "ignition_status",
  "position.speed": "speed",
  "vehicle.mileage": "mileage",
  "gsm.mcc": "mcc",
  "gsm.cellid": "cellID",
  "gsm.lac": "lacID",
  "position.altitude": "altitude",
};

/**
 * Convertidores por key
 */
const CONVERTERS = {
  "engine.ignition.status": b2i,
  ident: str,
  "protocol.version": str,
  "position.altitude": num,
  "position.latitude": num,
  "position.longitude": num,
  "position.speed": num,
  timestamp: unixSecondsToIsoUtc, // <-- recomendado (ISO UTC)
  // timestamp: unixSecondsToMySQLDateTime, // <-- si prefieres string "YYYY-MM-DD HH:mm:ss"
  "vehicle.mileage": num,
  "gsm.mcc": num,
  "gsm.cellid": num,
  "gsm.lac": num,
};

// Construye row SOLO con campos presentes en el JSON
function buildRowFromPayload(p) {
  const row = {};

  for (const [jsonKey, col] of Object.entries(KEY_TO_COL)) {
    if (!(jsonKey in p)) continue;

    const conv = CONVERTERS[jsonKey];
    const value = conv ? conv(p[jsonKey]) : p[jsonKey];

    if (value === undefined) continue;
    row[col] = value;
  }

  // Defaults / campos fijos como en tu código
  row.OperadorGps = "QL-GV300";
  row.imei = row.deviceID;
  row.bat = 100;
  row.battery = 100;
  row.lock_status = 1;
  row.insertDateTime = new Date().toISOString(); // mejor que string local/UTC ambiguo

  // Si NO quieres forzar mileage a 0, quita esto:
  row.mileage = row.mileage ?? 0;

  return row;
}

// Genera un ID estable para deduplicación en SQL (opcional pero recomendado)
function makeEventId(row, originalPayload) {
  // Ideal: deviceID + timestamp (segundos) o dateTime
  // Si tu payload trae timestamp original, úsalo.
  const device = row.deviceID || "noDevice";
  const dt = row.dateTime || "";
  // Puedes sumar lat/lon si quieres hacerlo aún más único.
  return `${device}_${dt}`;
}

// ===== Main =====
async function main() {
  // Healthcheck HTTP (por ejemplo para Render/Heroku/Azure App Service)
  http
    .createServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK");
    })
    .listen(process.env.PORT || 3000, () => {
      console.log(`✅ HTTP OK en puerto ${process.env.PORT || 3000}`);
    });

  // Producer de Event Hubs
  const producer = new EventHubProducerClient(
    EVENTHUB_CONNECTION_STRING,
    EVENTHUB_NAME
  );

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

    // Batch para performance
    let batch = await producer.createBatch();

    for (const p of items) {
      try {
        const row = buildRowFromPayload(p);

        // Exigir ident (deviceID) para evitar basura
        if (!row.deviceID) continue;

        const eventId = makeEventId(row, p);

        const eventBody = {
          ...row,
          eventId, // <-- útil para dedupe en SQL
          _topic,
          _receivedAtUtc: new Date().toISOString(),
        };

        // Si se llena, envía y crea otro batch
        const ok = batch.tryAdd({ body: eventBody });
        if (!ok) {
          await producer.sendBatch(batch);
          batch = await producer.createBatch();

          const ok2 = batch.tryAdd({ body: eventBody });
          if (!ok2) {
            console.error("❌ Evento demasiado grande para Event Hubs (no se pudo agregar al batch)");
          }
        }
      } catch (e) {
        console.error("❌ Error procesando payload:", e);
      }
    }

    if (batch.count > 0) {
      await producer.sendBatch(batch);
      // console.log(`✅ Enviados ${batch.count} eventos a Event Hubs`);
    }
  });

  client.on("error", (err) => console.error("MQTT error:", err));
  client.on("close", () => console.log("🔌 MQTT desconectado"));

  process.on("SIGINT", async () => {
    console.log("\n🛑 Cerrando...");
    try {
      client.end(true);
      await producer.close();
    } catch {}
    process.exit(0);
  });
}

main().catch((e) => {
  console.error("❌ Fatal:", e);
  process.exit(1);
});
