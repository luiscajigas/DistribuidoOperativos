import amqp from "amqplib";
export type RabbitMqOAuthConfig = {
    host: string;
    port: number;
    vhost?: string;
    getToken: () => Promise<string>;
    tls?: {
        ca?: string;
        cert?: string;
        key?: string;
        rejectUnauthorized?: boolean;
        servername?: string;
    };
};
export declare function connectRabbitMqWithOAuth(cfg: RabbitMqOAuthConfig): Promise<amqp.ChannelModel>;
//# sourceMappingURL=rabbitmq.d.ts.map