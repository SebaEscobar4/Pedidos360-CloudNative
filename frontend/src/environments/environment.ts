/**
 * Variables de entorno para Desarrollo
 * Reemplaza los placeholders con los valores de tu App Registration en Microsoft Entra ID (Azure AD).
 */
export const environment = {
  production: false,
  // URL base de la API Spring Boot (o AWS API Gateway si aplica)
  apiUrl: 'http://localhost:8080',
  azure: {
    // Client ID / Application ID registrado en Azure Portal para la SPA
    clientId: '268aca0b-2e3a-47a6-a806-903014bc7774',
    // Tenant ID (Directory ID) de tu suscripcion de Microsoft Azure
    tenantId: '670a4f6b-6666-47bc-9580-045077b9f328',
    // URI de redireccion configurada como Single-Page Application (SPA) en Azure
    redirectUri: 'http://localhost:4200/redirect',
    // Authority de Microsoft Entra ID (usa el tenantId o 'common')
    authority:
      'https://login.microsoftonline.com/670a4f6b-6666-47bc-9580-045077b9f328',
    // Scope delegado expuesto por el backend para crear pedidos
    apiScope: 'api://a063dd44-191e-4715-9b39-5923cc30c293/recurso.write',
    // Scopes solicitados en el login basico
    loginScopes: ['openid', 'profile', 'email'],
  },
  // Permite evaluar y alternar roles (ADMIN / CLIENTE) incluso sin tenant activo de Azure
  demoMode: true,
};
