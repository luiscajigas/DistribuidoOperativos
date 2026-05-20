export type TlsClientConfig = {
    ca?: string;
    cert?: string;
    key?: string;
    rejectUnauthorized?: boolean;
};
export declare function loadTlsConfigFromEnv(prefix: string): TlsClientConfig;
//# sourceMappingURL=tls.d.ts.map