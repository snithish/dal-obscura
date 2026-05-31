export const AUTH_PROVIDER_TYPES = {
  "default-jwt": {
    label: "Default JWT",
    module: "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter",
  },
} as const;

export type AuthProviderType = keyof typeof AUTH_PROVIDER_TYPES;

export type PathRule = {
  glob: string;
  allow: boolean;
};

export type RuntimeSettings = {
  ticket_ttl_seconds: number;
  max_tickets: number;
  max_ticket_exchanges: number;
  path_rules: PathRule[];
};

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

export type RuntimePayloadResult =
  | { ok: true; settings: RuntimeSettings }
  | { ok: false; error: string };

export function runtimePayloadFromForm(settings: RuntimeSettings): RuntimePayloadResult {
  const numericChecks: Array<[keyof RuntimeSettings, string]> = [
    ["ticket_ttl_seconds", "Ticket TTL seconds"],
    ["max_tickets", "Max tickets"],
    ["max_ticket_exchanges", "Max exchanges"],
  ];
  for (const [key, label] of numericChecks) {
    const value = settings[key];
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      return { ok: false, error: `${label} must be greater than zero.` };
    }
  }

  return {
    ok: true,
    settings: {
      ...settings,
      path_rules: settings.path_rules
        .map((rule) => ({ ...rule, glob: rule.glob.trim() }))
        .filter((rule) => rule.glob.length > 0),
    },
  };
}

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
