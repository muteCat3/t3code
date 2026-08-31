import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpServer } from "effect/unstable/http";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as OrchestrationMcpEligibility from "./OrchestrationMcpEligibility.ts";
import * as OrchestrationMcpInvocationContext from "./OrchestrationMcpInvocationContext.ts";
import * as OrchestrationMcpProviderSession from "./OrchestrationMcpProviderSession.ts";

export interface OrchestrationMcpCredentialRequest {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
}

export interface OrchestrationMcpIssuedCredential {
  readonly config: OrchestrationMcpProviderSession.OrchestrationMcpProviderSessionConfig;
}

export interface OrchestrationMcpSessionRegistryShape {
  readonly issue: (
    request: OrchestrationMcpCredentialRequest,
  ) => Effect.Effect<OrchestrationMcpIssuedCredential | undefined>;
  readonly resolve: (
    rawToken: string,
  ) => Effect.Effect<OrchestrationMcpInvocationContext.OrchestrationMcpInvocationScope | undefined>;
  readonly touch: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeProviderSession: (providerSessionId: string) => Effect.Effect<void>;
  readonly revokeThread: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeAll: Effect.Effect<void>;
}

export class OrchestrationMcpSessionRegistry extends Context.Service<
  OrchestrationMcpSessionRegistry,
  OrchestrationMcpSessionRegistryShape
>()("t3/mcp/OrchestrationMcpSessionRegistry") {}

interface CredentialRecord {
  readonly tokenHash: string;
  readonly scope: OrchestrationMcpInvocationContext.OrchestrationMcpInvocationScope;
  readonly lastAliveAt: number;
}

interface RegistryState {
  readonly records: ReadonlyMap<string, CredentialRecord>;
  readonly threadRevocations: ReadonlyMap<ThreadId, number>;
  readonly revokeAllGeneration: number;
}

export interface OrchestrationMcpSessionRegistryOptions {
  readonly livenessWindowMs?: number;
  readonly now?: () => number;
  readonly isEligible?: OrchestrationMcpEligibility.OrchestrationMcpEligibilityResolver;
}

const DEFAULT_LIVENESS_WINDOW_MS = 24 * 60 * 60 * 1_000;

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const tokenFromBytes = (bytes: Uint8Array): string => Buffer.from(bytes).toString("base64url");

const getHttpMcpEndpointHost = (hostname: string): string => {
  const normalized = hostname.toLowerCase();
  const endpointHostname =
    normalized === "0.0.0.0" || normalized === "::" || normalized === "[::]"
      ? "127.0.0.1"
      : hostname;
  return endpointHostname.includes(":") && !endpointHostname.startsWith("[")
    ? `[${endpointHostname}]`
    : endpointHostname;
};

const makeWithOptions = Effect.fn("OrchestrationMcpSessionRegistry.make")(function* (
  options: OrchestrationMcpSessionRegistryOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const httpServer = yield* HttpServer.HttpServer;
  const state = yield* SynchronizedRef.make<RegistryState>({
    records: new Map(),
    threadRevocations: new Map(),
    revokeAllGeneration: 0,
  });
  const currentTimeMillis = options.now ? Effect.sync(options.now) : Clock.currentTimeMillis;
  const livenessWindowMs = options.livenessWindowMs ?? DEFAULT_LIVENESS_WINDOW_MS;
  const isEligible =
    options.isEligible ?? OrchestrationMcpEligibility.checkActiveOrchestrationMcpEligibility;
  const endpoint =
    httpServer.address._tag === "TcpAddress"
      ? `http://${getHttpMcpEndpointHost(httpServer.address.hostname)}:${httpServer.address.port}/mcp/orchestration`
      : "http://127.0.0.1/mcp/orchestration";

  const hashToken = (token: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(token))
      .pipe(Effect.map(bytesToHex), Effect.orDie);

  const pruneDead = (records: ReadonlyMap<string, CredentialRecord>, timestamp: number) =>
    new Map(
      Array.from(records).filter(
        ([, record]) => timestamp - record.lastAliveAt <= livenessWindowMs,
      ),
    );

  const revokeWhere = (predicate: (record: CredentialRecord) => boolean) =>
    SynchronizedRef.update(state, (current) => ({
      ...current,
      records: new Map(Array.from(current.records).filter(([, record]) => !predicate(record))),
    }));

  const issue: OrchestrationMcpSessionRegistryShape["issue"] = Effect.fn(
    "OrchestrationMcpSessionRegistry.issue",
  )(function* (request) {
    if (!(yield* isEligible(request))) return undefined;
    const issuedAt = yield* currentTimeMillis;
    const providerSessionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
    const rawToken = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
    const tokenHash = yield* hashToken(rawToken);
    const admission = yield* SynchronizedRef.get(state).pipe(
      Effect.map((current) => ({
        threadRevocation: current.threadRevocations.get(request.threadId) ?? 0,
        revokeAllGeneration: current.revokeAllGeneration,
      })),
    );
    // Eligibility is intentionally checked again after credential generation.
    // A trust-off/archive transition may have committed and revoked while the
    // first projection read or random generation was in flight.
    if (!(yield* isEligible(request))) return undefined;
    const scope: OrchestrationMcpInvocationContext.OrchestrationMcpInvocationScope = {
      environmentId,
      threadId: ThreadId.make(request.threadId),
      providerSessionId,
      providerInstanceId: ProviderInstanceId.make(request.providerInstanceId),
      issuedAt,
    };
    const inserted = yield* SynchronizedRef.modify(state, (current) => {
      const next = pruneDead(current.records, issuedAt);
      if (
        current.revokeAllGeneration !== admission.revokeAllGeneration ||
        (current.threadRevocations.get(request.threadId) ?? 0) !== admission.threadRevocation
      ) {
        return [false, { ...current, records: next }] as const;
      }
      next.set(tokenHash, { tokenHash, scope, lastAliveAt: issuedAt });
      return [true, { ...current, records: next }] as const;
    });
    if (!inserted) return undefined;
    return {
      config: {
        environmentId,
        threadId: scope.threadId,
        providerSessionId,
        providerInstanceId: scope.providerInstanceId,
        endpoint,
        authorizationHeader: `Bearer ${rawToken}`,
      },
    };
  });

  const resolve: OrchestrationMcpSessionRegistryShape["resolve"] = Effect.fn(
    "OrchestrationMcpSessionRegistry.resolve",
  )(function* (rawToken) {
    if (rawToken.length === 0) return undefined;
    const tokenHash = yield* hashToken(rawToken);
    const timestamp = yield* currentTimeMillis;
    const record = yield* SynchronizedRef.modify(state, (state) => {
      const current = pruneDead(state.records, timestamp);
      return [current.get(tokenHash), { ...state, records: current }] as const;
    });
    if (!record) return undefined;
    const request = {
      threadId: record.scope.threadId,
      providerInstanceId: record.scope.providerInstanceId,
    };
    if (!(yield* isEligible(request))) {
      yield* revokeWhere((candidate) => candidate.tokenHash === tokenHash);
      return undefined;
    }
    const retained = yield* SynchronizedRef.modify(state, (current) => {
      const live = current.records.get(tokenHash);
      if (live === undefined) return [false, current] as const;
      const next = new Map(current.records);
      next.set(tokenHash, { ...live, lastAliveAt: timestamp });
      return [true, { ...current, records: next }] as const;
    });
    return retained ? record.scope : undefined;
  });

  const touch: OrchestrationMcpSessionRegistryShape["touch"] = Effect.fn(
    "OrchestrationMcpSessionRegistry.touch",
  )(function* (threadId) {
    const timestamp = yield* currentTimeMillis;
    yield* SynchronizedRef.update(state, (state) => {
      const records = pruneDead(state.records, timestamp);
      const next = new Map(records);
      for (const [tokenHash, record] of records) {
        if (record.scope.threadId === threadId) {
          next.set(tokenHash, { ...record, lastAliveAt: timestamp });
        }
      }
      return { ...state, records: next };
    });
  });

  return OrchestrationMcpSessionRegistry.of({
    issue,
    resolve,
    touch,
    revokeProviderSession: Effect.fn("OrchestrationMcpSessionRegistry.revokeProviderSession")(
      function* (providerSessionId) {
        yield* revokeWhere((record) => record.scope.providerSessionId === providerSessionId);
      },
    ),
    revokeThread: Effect.fn("OrchestrationMcpSessionRegistry.revokeThread")(function* (threadId) {
      yield* SynchronizedRef.update(state, (current) => {
        const threadRevocations = new Map(current.threadRevocations);
        threadRevocations.set(threadId, (threadRevocations.get(threadId) ?? 0) + 1);
        return {
          ...current,
          threadRevocations,
          records: new Map(
            Array.from(current.records).filter(([, record]) => record.scope.threadId !== threadId),
          ),
        };
      });
    }),
    revokeAll: SynchronizedRef.update(state, (current) => ({
      ...current,
      records: new Map(),
      revokeAllGeneration: current.revokeAllGeneration + 1,
    })),
  });
});

let activeRegistry: OrchestrationMcpSessionRegistryShape | undefined;

const make = Effect.acquireRelease(
  makeWithOptions().pipe(
    Effect.tap((registry) =>
      Effect.sync(() => {
        activeRegistry = registry;
      }),
    ),
  ),
  (registry) =>
    Effect.sync(() => {
      if (activeRegistry === registry) activeRegistry = undefined;
    }),
);

export const layer = Layer.effect(OrchestrationMcpSessionRegistry, make);

export const issueActiveOrchestrationMcpCredential = (
  request: OrchestrationMcpCredentialRequest,
): Effect.Effect<OrchestrationMcpIssuedCredential | undefined> =>
  activeRegistry
    ? activeRegistry
        .revokeThread(request.threadId)
        .pipe(Effect.andThen(activeRegistry.issue(request)))
    : Effect.succeed(undefined);

export const touchActiveOrchestrationMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeRegistry ? activeRegistry.touch(threadId) : Effect.void;

export const revokeActiveOrchestrationMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeRegistry ? activeRegistry.revokeThread(threadId) : Effect.void;

export const revokeAllActiveOrchestrationMcpCredentials = (): Effect.Effect<void> =>
  activeRegistry ? activeRegistry.revokeAll : Effect.void;

export const __testing = { make: makeWithOptions };
