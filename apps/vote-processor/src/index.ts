import { AuthClient, connectRabbitMqWithOAuth, createKafkaWithOAuth, loadTlsConfigFromEnv } from "@zt/common";

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

type ProcessedVote = RawVote & {
  processed_at: number;
};

async function main() {
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
    clientId: "vote-processor",
    brokers: kafkaBrokers,
    ssl: kafkaTls.ca
      ? { ca: [kafkaTls.ca], rejectUnauthorized: kafkaTls.rejectUnauthorized }
      : undefined,
    getToken: () => auth.getAccessToken()
  });

  // Fanout exchange para dashboards
  const rabbitHost = mustGetEnv("RABBIT_HOST");
  const rabbitPort = Number(process.env.RABBIT_PORT ?? "5672");
  const rabbitTls = loadTlsConfigFromEnv("RABBIT_TLS");

  const amqpConn = await connectRabbitMqWithOAuth({
    host: rabbitHost,
    port: rabbitPort,
    vhost: process.env.RABBIT_VHOST ?? "/",
    getToken: () => auth.getAccessToken(),
    tls: {
      ca: rabbitTls.ca,
      rejectUnauthorized: rabbitTls.rejectUnauthorized
    }
  });
  const channel = await amqpConn.createChannel();

  // Declarar fanout exchange para que los dashboards reciban votos
  await channel.assertExchange("votes_fanout", "fanout", { durable: true });

  const producer = kafka.producer();
  await producer.connect();

  // KTable de votos ya procesados (deduplicación)
  const processedUsers = new Set<string>();

  const consumer = kafka.consumer({ groupId: "vote-processor" });
  await consumer.connect();
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

      if (!raw.user_id || !raw.vote) return;

      // Deduplicar: si el user_id ya votó, descartar
      if (processedUsers.has(raw.user_id)) {
        process.stdout.write(`[vote-processor] duplicate vote from ${raw.user_id}, discarding\n`);
        return;
      }
      processedUsers.add(raw.user_id);

      const processed: ProcessedVote = {
        ...raw,
        processed_at: Date.now()
      };

      const msg = Buffer.from(JSON.stringify(processed));

      // Publicar a Kafka para procesadores stateful (rollup, archiver)
      await producer.send({
        topic: "processed_votes",
        messages: [
          {
            key: processed.user_id,
            value: msg,
            headers: { "content-type": Buffer.from("application/json", "utf8") }
          }
        ]
      });

      // Publicar al fanout para dashboards en tiempo real
      channel.publish("votes_fanout", "", msg, {
        contentType: "application/json",
        persistent: false
      });

      process.stdout.write(`[vote-processor] processed vote from ${raw.user_id} (${raw.vote})\n`);
    }
  });
}

main().catch((err) => {
  process.stderr.write(`${String(err)}\n`);
  process.exit(1);
});
