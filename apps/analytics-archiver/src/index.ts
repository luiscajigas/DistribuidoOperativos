import fs from "node:fs";
import path from "node:path";
import { AuthClient, createKafkaWithOAuth, loadTlsConfigFromEnv } from "@zt/common";

function mustGetEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

type ProcessedVote = {
  user_id: string;
  vote: string;
  region: string;
  ts: number;
  processed_at: number;
};

async function main() {
  const archiveDir = process.env.ARCHIVE_DIR ?? "/opt/zero-trust/archive";
  const archiveFile = path.join(archiveDir, "votes.jsonl");
  const flushInterval = Number(process.env.FLUSH_INTERVAL_MS ?? "2000");

  // Crear directorio si no existe
  fs.mkdirSync(archiveDir, { recursive: true });

  const authTls = loadTlsConfigFromEnv("AUTH_TLS");
  const auth = new AuthClient({
    authBaseUrl: mustGetEnv("AUTH_BASE_URL"),
    clientId: mustGetEnv("CLIENT_ID"),
    clientSecret: mustGetEnv("CLIENT_SECRET"),
    ca: authTls.ca
  });

  const kafkaBrokers = mustGetEnv("KAFKA_BROKERS").split(",").filter(Boolean);
  const kafkaTls = loadTlsConfigFromEnv("KAFKA_TLS");

  const kafka = createKafkaWithOAuth({
    clientId: "analytics-archiver",
    brokers: kafkaBrokers,
    ssl: kafkaTls.ca
      ? { ca: [kafkaTls.ca], rejectUnauthorized: kafkaTls.rejectUnauthorized }
      : undefined,
    getToken: () => auth.getAccessToken()
  });

  // Buffer en memoria para escritura por lotes
  const buffer: string[] = [];

  const consumer = kafka.consumer({ groupId: "analytics-archiver" });
  await consumer.connect();
  await consumer.subscribe({ topic: "processed_votes", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      let vote: ProcessedVote;
      try {
        vote = JSON.parse(message.value.toString("utf8")) as ProcessedVote;
      } catch {
        return;
      }

      // Formato JSONL: una línea por voto
      buffer.push(JSON.stringify(vote));
    }
  });

  // Flush periódico al disco
  setInterval(() => {
    if (buffer.length === 0) return;

    const lines = buffer.splice(0, buffer.length);
    fs.appendFileSync(archiveFile, lines.join("\n") + "\n", "utf8");
    process.stdout.write(`[analytics-archiver] flushed ${lines.length} votes to ${archiveFile}\n`);
  }, flushInterval);

  // Flush al cerrar
  process.on("SIGTERM", () => {
    if (buffer.length > 0) {
      fs.appendFileSync(archiveFile, buffer.join("\n") + "\n", "utf8");
    }
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`${String(err)}\n`);
  process.exit(1);
});
