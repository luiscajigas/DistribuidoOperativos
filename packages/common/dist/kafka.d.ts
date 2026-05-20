import { Kafka, type KafkaConfig } from "kafkajs";
export type KafkaOAuthConfig = {
    brokers: string[];
    clientId: string;
    ssl: KafkaConfig["ssl"];
    getToken: () => Promise<string>;
};
export declare function createKafkaWithOAuth(cfg: KafkaOAuthConfig): Kafka;
//# sourceMappingURL=kafka.d.ts.map