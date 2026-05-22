import express from "express";
import { AuthClient, connectRabbitMqWithOAuth, loadTlsConfigFromEnv } from "@zt/common";

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
  const port = Number(process.env.PORT ?? "8080");

  const authTls = loadTlsConfigFromEnv("AUTH_TLS");
  const auth = new AuthClient({
    authBaseUrl: mustGetEnv("AUTH_BASE_URL"),
    clientId: mustGetEnv("CLIENT_ID"),
    clientSecret: mustGetEnv("CLIENT_SECRET"),
    ca: authTls.ca
  });

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

  // Unirse al fanout que publica vote_processor
  await channel.assertExchange("votes_fanout", "fanout", { durable: true });
  const q = await channel.assertQueue("", { exclusive: true, autoDelete: true });
  await channel.bindQueue(q.queue, "votes_fanout", "");

  // Estado global en memoria
  const globalCounts: Record<string, number> = {};
  let totalVotes = 0;
  let lastUpdated = 0;

  await channel.consume(
    q.queue,
    (msg) => {
      if (!msg) return;
      try {
        const vote = JSON.parse(msg.content.toString("utf8")) as ProcessedVote;
        if (!vote.vote) return;
        globalCounts[vote.vote] = (globalCounts[vote.vote] ?? 0) + 1;
        totalVotes++;
        lastUpdated = Date.now();
      } catch {
        // ignorar mensajes malformados
      }
    },
    { noAck: true }
  );

  const app = express();

  app.get("/health", (_req, res) => {
    res.status(200).json({ ok: true });
  });

  app.get("/dashboard", (_req, res) => {
    res.status(200).json({
      total_votes: totalVotes,
      counts: globalCounts,
      last_updated: lastUpdated,
      percentages: Object.fromEntries(
        Object.entries(globalCounts).map(([k, v]) => [
          k,
          totalVotes > 0 ? Number(((v / totalVotes) * 100).toFixed(2)) : 0
        ])
      )
    });
  });

  app.listen(port, "0.0.0.0", () => {
    process.stdout.write(`[global-dashboard] listening on :${port}\n`);
  });
}

main().catch((err) => {
  process.stderr.write(`${String(err)}\n`);
  process.exit(1);
});
