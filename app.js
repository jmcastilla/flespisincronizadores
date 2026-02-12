/**
 * MySQL -> Azure Event Hubs
 * - cada 1 minuto: lee hasta 1000 filas donde actualizado3=0
 * - envía a Event Hub en batch
 * - si envío OK: marca actualizado3=1 por maindataID
 *
 * npm i dotenv mysql2 @azure/event-hubs
 */

require("dotenv").config();

const http = require("http");
const mysql = require("mysql2/promise");
const { EventHubProducerClient } = require("@azure/event-hubs");

const {
  MYSQL_HOST,
  MYSQL_PORT = 3306,
  MYSQL_USER,
  MYSQL_PASSWORD,
  MYSQL_DATABASE = "PlacasGps",
  EVENTHUB_CONNECTION_STRING,
  EVENTHUB_NAME,
  PORT = 3000,
} = process.env;

if (!MYSQL_HOST) throw new Error("Falta MYSQL_HOST");
if (!MYSQL_USER) throw new Error("Falta MYSQL_USER");
if (!MYSQL_DATABASE) throw new Error("Falta MYSQL_DATABASE");
if (!EVENTHUB_CONNECTION_STRING) throw new Error("Falta EVENTHUB_CONNECTION_STRING");
if (!EVENTHUB_NAME) throw new Error("Falta EVENTHUB_NAME");

// ===== Ajustes =====
const BATCH_DB_LIMIT = 1000;      // equivale a "top 1000"
const UPDATE_CHUNK = 500;         // ids por UPDATE (para no pasar límites de placeholders)
const INTERVAL_MS = 60_000;        // 1 minuto

// En MySQL, conviene ordenar para procesar establemente
const SELECT_SQL = `
SELECT
  maindataID, deviceID, OperadorGps, imei, dateTime, insertDateTime,
  bat, battery, latitude, longitude, speed,
  CAST(lock_status AS UNSIGNED) AS lock_status,
  CAST(ignition_status AS UNSIGNED) AS ignition_status
FROM PlacasGps.mainData
WHERE actualizado3 = 0
ORDER BY maindataID ASC
LIMIT ?
`;

function toIsoIfDate(v) {
  return v instanceof Date ? v.toISOString() : v;
}

// Puedes agregar/ajustar normalización aquí
function transformRow(r) {
  const out = { ...r };
  out.dateTime = toIsoIfDate(out.dateTime);
  out.insertDateTime = toIsoIfDate(out.insertDateTime);

  // eventId útil para dedupe (opcional)
  const device = out.deviceID || out.imei || "noDevice";
  out.eventId = `${device}_${out.dateTime || ""}_${out.maindataID}`;

  out._sentAtUtc = new Date().toISOString();
  return out;
}

async function updateActualizado3(conn, ids) {
  if (!ids.length) return 0;

  let updated = 0;
  for (let i = 0; i < ids.length; i += UPDATE_CHUNK) {
    const chunk = ids.slice(i, i + UPDATE_CHUNK);
    const placeholders = chunk.map(() => "?").join(",");
    const sql = `
      UPDATE PlacasGps.mainData
      SET actualizado3 = 1
      WHERE maindataID IN (${placeholders})
    `;
    const [res] = await conn.execute(sql, chunk);
    updated += res.affectedRows || 0;
  }
  return updated;
}

async function main() {
  // Healthcheck
  http
    .createServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK");
    })
    .listen(Number(PORT), () => {
      console.log(`✅ HTTP OK en puerto ${PORT}`);
    });

  const pool = mysql.createPool({
    host: MYSQL_HOST,
    port: Number(MYSQL_PORT),
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
  });

  const producer = new EventHubProducerClient(
    EVENTHUB_CONNECTION_STRING,
    EVENTHUB_NAME
  );

  let running = false;

  async function tick() {
    if (running) {
      console.log("⏭️ Tick omitido: todavía hay un envío en curso.");
      return;
    }
    running = true;

    const started = Date.now();
    let conn;

    try {
      conn = await pool.getConnection();

      const [rows] = await conn.execute(SELECT_SQL, [BATCH_DB_LIMIT]);

      if (!rows.length) {
        console.log(`⏳ Sin pendientes | ${new Date().toISOString()}`);
        return;
      }

      console.log(`📥 Pendientes leídos=${rows.length} | ${new Date().toISOString()}`);

      // Envío a EventHub (batch por tamaño)
      let batch = await producer.createBatch();
      let sent = 0;
      let skipped = 0;

      const idsToMark = [];

      let loggedSample = false;

      for (const r of rows) {
        const body = transformRow(r);

        if (!loggedSample) {
          console.log("🧾 Ejemplo evento:", body);
          loggedSample = true;
        }

        const ok = batch.tryAdd({ body });
        if (!ok) {
          // manda el batch actual
          await producer.sendBatch(batch);
          sent += batch.count;

          batch = await producer.createBatch();
          const ok2 = batch.tryAdd({ body });
          if (!ok2) {
            console.error("❌ Evento demasiado grande, se omite. maindataID=", r.maindataID);
            skipped++;
            continue;
          }
        }

        idsToMark.push(r.maindataID);
      }

      if (batch.count > 0) {
        await producer.sendBatch(batch);
        sent += batch.count;
      }

      // ✅ Solo si el envío fue OK, marcamos actualizado3=1
      const updated = await updateActualizado3(conn, idsToMark);

      console.log(
        `✅ OK | enviados=${sent} | omitidos=${skipped} | actualizados=${updated} | ms=${Date.now() - started}`
      );
    } catch (e) {
      console.error("❌ Error tick:", e);
      // Importante: si falló envío o update, NO marcamos actualizado3
    } finally {
      if (conn) conn.release();
      running = false;
    }
  }

  // correr al iniciar y luego cada 1 min
  await tick();
  const interval = setInterval(() => tick().catch(console.error), INTERVAL_MS);

  process.on("SIGINT", async () => {
    console.log("\n🛑 Cerrando...");
    clearInterval(interval);
    try {
      await producer.close();
      await pool.end();
    } catch {}
    process.exit(0);
  });
}

main().catch((e) => {
  console.error("❌ Fatal:", e);
  process.exit(1);
});
