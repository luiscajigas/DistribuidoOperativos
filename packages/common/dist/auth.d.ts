export type TokenResponse = {
    access_token: string;
    token_type: "Bearer";
    expires_in: number;
};
export type AuthClientConfig = {
    authBaseUrl: string;
    clientId: string;
    clientSecret: string;
    ca?: string;
};
export declare class AuthClient {
    private cache?;
    private readonly cfg;
    constructor(cfg: AuthClientConfig);
    getAccessToken(): Promise<string>;
}
//# sourceMappingURL=auth.d.ts.map