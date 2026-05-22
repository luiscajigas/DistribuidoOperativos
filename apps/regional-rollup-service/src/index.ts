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

type RegionalResult = {
  region: string;
  counts: Record<string, number>;
  total: number;
  last_updated: number;
};

async function main() {
  // Intervalo de publicación de resultados (ms)
  const PUBLISH_INTERVAL_MS = Number(process.env.PUBLISH_INTERVAL_MS ?? "5000");

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
    clientId: "regional-rollup-service",
    brokers: kafkaBrokers,
    ssl: kafkaTls.ca
      ? { ca: [kafkaTls.ca], rejectUnauthorized: kafkaTls.rejectUnauthorized }
      : undefined,
    getToken: () => auth.getAccessToken()
  });

  const producer = kafka.producer();
  await producer.connect();

  // KTable: region → { candidate → count }
  const regionTable = new Map<string, Map<string, number>>();

  const consumer = kafka.consumer({ groupId: "regional-rollup-service" });
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

      if (!vote.region || !vote.vote) return;

      if (!regionTable.has(vote.region)) {
        regionTable.set(vote.region, new Map());
      }
      const regionCounts = regionTable.get(vote.region)!;
      regionCounts.set(vote.vote, (regionCounts.get(vote.vote) ?? 0) + 1);
    }
  });

  // Publicar snapshot de cada región periódicamente
  setInterval(async () => {
    if (regionTable.size === 0) return;

    const messages = Array.from(regionTable.entries()).map(([region, counts]) => {
      const countsObj: Record<string, number> = {};
      counts.forEach((v, k) => { countsObj[k] = v; });

      const result: RegionalResult = {
        region,
        counts: countsObj,
        total: Array.from(counts.values()).reduce((a, b) => a + b, 0),
        last_updated: Date.now()
      };

      return {
        key: region,
        value: JSON.stringify(result),
        headers: { "content-type": Buffer.from("application/json", "utf8") }
      };
    });

    await producer.send({ topic: "regional_results", messages });
    process.stdout.write(`[regional-rollup] published results for ${messages.length} regions\n`);
  }, PUBLISH_INTERVAL_MS);
}

main().catch((err) => {
  process.stderr.write(`${String(err)}\n`);
  process.exit(1);
});
