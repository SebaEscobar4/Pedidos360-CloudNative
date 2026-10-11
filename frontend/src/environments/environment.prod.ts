export const environment = {
  production: true,

  apiUrl: 'https://n4tt1vgkq0.execute-api.us-east-1.amazonaws.com',

  azure: {
    clientId: '268aca0b-2e3a-47a6-a806-903014bc7774',
    tenantId: '670a4f6b-6666-47bc-9580-045077b9f328',

    redirectUri: 'https://sebaescobar4.github.io/Pedidos360-CloudNative/redirect',

    authority:
      'https://login.microsoftonline.com/670a4f6b-6666-47bc-9580-045077b9f328/v2.0',

    apiScope: 'api://a063dd44-191e-4715-9b39-5923cc30c293/recurso.write',

    loginScopes: ['openid', 'profile', 'email'],
  },

  demoMode: false,

};