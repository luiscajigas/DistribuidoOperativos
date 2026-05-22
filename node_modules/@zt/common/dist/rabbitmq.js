import amqp from "amqplib";
export async function connectRabbitMqWithOAuth(cfg) {
    const token = await cfg.getToken();
    const vhost = cfg.vhost ?? "/";
    const url = `amqps://ignored:${encodeURIComponent(token)}` +
        `@${cfg.host}:${cfg.port}${vhost === "/" ? "" : `/${encodeURIComponent(vhost)}`}`;
    const socketOptions = {
        ca: cfg.tls?.ca,
        cert: cfg.tls?.cert,
        key: cfg.tls?.key,
        rejectUnauthorized: cfg.tls?.rejectUnauthorized ?? true,
        servername: cfg.tls?.servername
    };
    return amqp.connect(url, socketOptions);
}
