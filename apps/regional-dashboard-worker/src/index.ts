import express from "express";
import { AuthClient, createKafkaWithOAuth, loadTlsConfigFromEnv } from "@zt/common";

function mustGetEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

type RegionalResult = {
  region: string;
  counts: Record<string, number>;
  total: number;
  last_updated: number;
};

async function main() {
  const port = Number(process.env.PORT ?? "8081");

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
    clientId: "regional-dashboard-worker",
    brokers: kafkaBrokers,
    ssl: kafkaTls.ca
      ? { ca: [kafkaTls.ca], rejectUnauthorized: kafkaTls.rejectUnauthorized }
      : undefined,
    getToken: () => auth.getAccessToken()
  });

  // KTable regional: region → último snapshot publicado por regional_rollup
  const regionData = new Map<string, RegionalResult>();

  const consumer = kafka.consumer({ groupId: "regional-dashboard-worker" });
  await consumer.connect();
  await consumer.subscribe({ topic: "regional_results", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const result = JSON.parse(message.value.toString("utf8")) as RegionalResult;
        if (!result.region) return;
        regionData.set(result.region, result);
      } catch {
        return;
      }
    }
  });

  const app = express();

  app.get("/health", (_req, res) => {
    res.status(200).json({ ok: true });
  });

  // Lista todas las regiones disponibles
  app.get("/dashboard", (_req, res) => {
    const regions = Array.from(regionData.keys());
    const summary = regions.map((r) => {
      const d = regionData.get(r)!;
      return { region: r, total: d.total, last_updated: d.last_updated };
    });
    res.status(200).json({ regions: summary });
  });

  // Detalle de una región
  app.get("/dashboard/:region", (req, res) => {
    const data = regionData.get(req.params.region);
    if (!data) {
      res.status(404).json({ error: "region_not_found" });
      return;
    }

    const percentages = Object.fromEntries(
      Object.entries(data.counts).map(([k, v]) => [
        k,
        data.total > 0 ? Number(((v / data.total) * 100).toFixed(2)) : 0
      ])
    );

    res.status(200).json({ ...data, percentages });
  });

  app.listen(port, "0.0.0.0", () => {
    process.stdout.write(`[regional-dashboard] listening on :${port}\n`);
  });
}

main().catch((err) => {
  process.stderr.write(`${String(err)}\n`);
  process.exit(1);
});
