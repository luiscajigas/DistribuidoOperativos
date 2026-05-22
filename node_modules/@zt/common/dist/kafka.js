import { Kafka } from "kafkajs";
export function createKafkaWithOAuth(cfg) {
    const sasl = {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
            const token = await cfg.getToken();
            return { value: token };
        }
    };
    return new Kafka({
        clientId: cfg.clientId,
        brokers: cfg.brokers,
        ssl: cfg.ssl,
        sasl
    });
}
