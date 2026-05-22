import { AuthClient, createKafkaWithOAuth, loadTlsConfigFromEnv } from "@zt/common";

function mustGetEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

type RawVote = {
  user_id: string;
  vote: string;
  region: string;
  ts: number;
};

type BotAlert = {
  user_id: string;
  first_ts: number;
  second_ts: number;
  gap_ms: number;
  alert: "rapid_duplicate_vote";
};

async function main() {
  const BOT_WINDOW_MS = Number(process.env.BOT_WINDOW_MS ?? "5000");

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
    clientId: "bot-detector-service",
    brokers: kafkaBrokers,
    ssl: kafkaTls.ca
      ? { ca: [kafkaTls.ca], rejectUnauthorized: kafkaTls.rejectUnauthorized }
      : undefined,
    getToken: () => auth.getAccessToken()
  });

  const producer = kafka.producer();
  await producer.connect();

  // Registro: user_id → timestamp del primer voto visto
  const firstSeenAt = new Map<string, number>();

  const consumer = kafka.consumer({ groupId: "bot-detector-service" });
  await consumer.connect();
  // Lee raw_votes (antes de deduplicar) para detectar intentos rápidos
  await consumer.subscribe({ topic: "raw_votes", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      let raw: RawVote;
      try {
        raw = JSON.parse(message.value.toString("utf8")) as RawVote;
      } catch {
        return;
      }

      if (!raw.user_id) return;

      const now = raw.ts ?? Date.now();
      const prev = firstSeenAt.get(raw.user_id);

      if (prev === undefined) {
        firstSeenAt.set(raw.user_id, now);
        return;
      }

      const gap = now - prev;

      if (gap < BOT_WINDOW_MS) {
        const alert: BotAlert = {
          user_id: raw.user_id,
          first_ts: prev,
          second_ts: now,
          gap_ms: gap,
          alert: "rapid_duplicate_vote"
        };

        await producer.send({
          topic: "bot_alerts",
          messages: [
            {
              key: raw.user_id,
              value: JSON.stringify(alert),
              headers: { "content-type": Buffer.from("application/json", "utf8") }
            }
          ]
        });

        process.stdout.write(
          `[bot-detector] ALERT: ${raw.user_id} voted twice in ${gap}ms\n`
        );
      }

      // Actualizar siempre con el timestamp más reciente
      firstSeenAt.set(raw.user_id, now);
    }
  });
}

main().catch((err) => {
  process.stderr.write(`${String(err)}\n`);
  process.exit(1);
});
