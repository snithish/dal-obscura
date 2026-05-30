export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

const TOKEN_STORAGE_KEY = "dal-obscura.adminToken";

function adminHeaders(): HeadersInit {
  const token = window.localStorage.getItem(TOKEN_STORAGE_KEY) ?? "dev-admin";
  return {
    Accept: "application/json",
    Authorization: `Bearer ${token}`,
  };
}

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: adminHeaders(),
  });
  if (!response.ok) {
    throw new ApiError(`Request failed: ${path}`, response.status);
  }
  return (await response.json()) as T;
}

export async function apiPut<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    body: JSON.stringify(body),
    headers: {
      ...adminHeaders(),
      "Content-Type": "application/json",
    },
    method: "PUT",
  });
  if (!response.ok) {
    throw new ApiError(`Request failed: ${path}`, response.status);
  }
  return (await response.json()) as T;
}
