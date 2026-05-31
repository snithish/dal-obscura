import { describe, expect, test } from "vitest";

import {
  AUTH_PROVIDER_TYPES,
  authProviderFormFromProvider,
  authProviderPayloadFromForm,
  type AuthProvider,
  type AuthProviderForm,
} from "./settingsLogic";

describe("auth provider form mapping", () => {
  test("uses a provider type choice to build the backend module payload", () => {
    const form: AuthProviderForm = {
      enabled: true,
      jwtSecretEnv: "DAL_OBSCURA_JWT_SECRET",
      ordinal: 1,
      providerType: "default-jwt",
    };

    expect(authProviderPayloadFromForm(form)).toEqual({
      args: { jwt_secret: { secret: "DAL_OBSCURA_JWT_SECRET" } },
      enabled: true,
      module: AUTH_PROVIDER_TYPES["default-jwt"].module,
      ordinal: 1,
    });
  });

  test("hydrates a friendly provider form from saved backend settings", () => {
    const provider: AuthProvider = {
      args: { jwt_secret: { secret: "DAL_OBSCURA_PLATFORM_JWT_SECRET" } },
      enabled: false,
      module: AUTH_PROVIDER_TYPES["default-jwt"].module,
      ordinal: 2,
    };

    expect(authProviderFormFromProvider(provider)).toEqual({
      enabled: false,
      jwtSecretEnv: "DAL_OBSCURA_PLATFORM_JWT_SECRET",
      ordinal: 2,
      providerType: "default-jwt",
    });
  });
});
