import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

type RouteContext = { params: Promise<{ path: string[] }> };

async function forward(request: NextRequest, context: RouteContext) {
  const { path } = await context.params;
  const upstreamBase = process.env.ATLAS_API_URL ?? "http://localhost:8000";
  const upstream = new URL(`${upstreamBase.replace(/\/$/, "")}/${path.join("/")}`);
  request.nextUrl.searchParams.forEach((value, key) => upstream.searchParams.append(key, value));

  const headers = new Headers({ Accept: "application/json" });
  const contentType = request.headers.get("content-type");
  if (contentType) headers.set("Content-Type", contentType);
  const apiKey = process.env.ATLAS_API_KEY;
  if (apiKey) headers.set("X-Atlas-API-Key", apiKey);
  const requestId = request.headers.get("x-request-id");
  if (requestId) headers.set("X-Request-ID", requestId);

  try {
    const response = await fetch(upstream, {
      method: request.method,
      headers,
      body: request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer(),
      cache: "no-store",
      signal: AbortSignal.timeout(request.method === "GET" ? 8_000 : 25_000),
    });
    const responseHeaders = new Headers();
    for (const name of ["content-type", "x-request-id", "x-atlas-idempotent-replay"]) {
      const value = response.headers.get(name);
      if (value) responseHeaders.set(name, value);
    }
    return new Response(response.body, { status: response.status, headers: responseHeaders });
  } catch (reason) {
    const timedOut = reason instanceof Error && reason.name === "TimeoutError";
    return Response.json({ detail: timedOut ? "Atlas took too long to respond" : "Atlas API is unavailable" }, { status: timedOut ? 504 : 502 });
  }
}

export const GET = forward;
export const POST = forward;
