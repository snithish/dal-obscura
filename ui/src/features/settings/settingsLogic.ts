export const AUTH_PROVIDER_TYPES = {
  "default-jwt": {
    label: "Default JWT",
    module: "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter",
  },
} as const;

export type AuthProviderType = keyof typeof AUTH_PROVIDER_TYPES;

export type AuthProvider = {
  id?: string;
  ordinal: number;
  module: string;
  args: Record<string, unknown>;
  enabled: boolean;
};

export type AuthProviderForm = {
  enabled: boolean;
  jwtSecretEnv: string;
  ordinal: number;
  providerType: AuthProviderType;
};

export const defaultAuthProvider: AuthProviderForm = {
  enabled: true,
  jwtSecretEnv: "DAL_OBSCURA_JWT_SECRET",
  ordinal: 1,
  providerType: "default-jwt",
};

export function authProviderPayloadFromForm(form: AuthProviderForm): AuthProvider {
  return {
    args: { jwt_secret: { secret: form.jwtSecretEnv } },
    enabled: form.enabled,
    module: AUTH_PROVIDER_TYPES[form.providerType].module,
    ordinal: form.ordinal,
  };
}

export function authProviderFormFromProvider(provider: AuthProvider): AuthProviderForm {
  return {
    enabled: provider.enabled,
    jwtSecretEnv: readJwtSecretEnv(provider.args),
    ordinal: provider.ordinal,
    providerType: providerTypeFromModule(provider.module),
  };
}

function providerTypeFromModule(module: string): AuthProviderType {
  const match = Object.entries(AUTH_PROVIDER_TYPES).find(
    ([, provider]) => provider.module === module,
  );
  return (match?.[0] as AuthProviderType | undefined) ?? "default-jwt";
}

function readJwtSecretEnv(args: Record<string, unknown>): string {
  const jwtSecret = args.jwt_secret;
  if (
    jwtSecret &&
    typeof jwtSecret === "object" &&
    "secret" in jwtSecret &&
    typeof jwtSecret.secret === "string"
  ) {
    return jwtSecret.secret;
  }
  return defaultAuthProvider.jwtSecretEnv;
}
