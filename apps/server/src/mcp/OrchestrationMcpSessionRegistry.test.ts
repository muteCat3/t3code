import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { HttpServer } from "effect/unstable/http";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as OrchestrationMcpSessionRegistry from "./OrchestrationMcpSessionRegistry.ts";

const environmentId = EnvironmentId.make("environment-1");
const fakeHttpServer = HttpServer.HttpServer.of({
  address: { _tag: "TcpAddress", hostname: "0.0.0.0", port: 43123 },
  serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
});
const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(environmentId),
  getDescriptor: Effect.die("unused"),
});

const makeRegistryWithEligibility = (
  isEligible: OrchestrationMcpSessionRegistry.OrchestrationMcpSessionRegistryOptions["isEligible"],
) =>
  OrchestrationMcpSessionRegistry.__testing
    .make({
      now: () => 1_000,
      ...(isEligible === undefined ? {} : { isEligible }),
    })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, fakeHttpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      Effect.provide(NodeServices.layer),
    );

const makeRegistry = (isEligible: () => boolean) =>
  makeRegistryWithEligibility(() => Effect.sync(isEligible));

it.effect("issues only eligible orchestration credentials at the isolated endpoint", () =>
  Effect.gen(function* () {
    let eligible = false;
    const registry = yield* makeRegistry(() => eligible);
    const request = {
      threadId: ThreadId.make("root-thread"),
      providerInstanceId: ProviderInstanceId.make("codex-main"),
    };

    expect(yield* registry.issue(request)).toBeUndefined();

    eligible = true;
    const issued = yield* registry.issue(request);
    expect(issued?.config.endpoint).toBe("http://127.0.0.1:43123/mcp/orchestration");
    expect(issued?.config.authorizationHeader).toMatch(/^Bearer [A-Za-z0-9_-]+$/);
  }),
);

it.effect("rechecks live eligibility on every resolution and revokes on failure", () =>
  Effect.gen(function* () {
    let eligible = true;
    const registry = yield* makeRegistry(() => eligible);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("root-thread"),
      providerInstanceId: ProviderInstanceId.make("codex-main"),
    });
    const token = issued?.config.authorizationHeader.replace(/^Bearer\s+/, "") ?? "";

    expect((yield* registry.resolve(token))?.threadId).toBe("root-thread");
    eligible = false;
    expect(yield* registry.resolve(token)).toBeUndefined();
    eligible = true;
    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect("does not accept a credential issued by the preview registry", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => true);
    expect(yield* registry.resolve("preview-registry-token")).toBeUndefined();
  }),
);

it.effect("keeps preview and orchestration bearer tokens mutually isolated", () =>
  Effect.gen(function* () {
    const orchestration = yield* makeRegistry(() => true);
    const preview = yield* McpSessionRegistry.__testing
      .make({ now: () => 1_000 })
      .pipe(
        Effect.provideService(HttpServer.HttpServer, fakeHttpServer),
        Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
        Effect.provide(NodeServices.layer),
      );
    const request = {
      threadId: ThreadId.make("root-thread"),
      providerInstanceId: ProviderInstanceId.make("codex-main"),
    };
    const orchestrationToken =
      (yield* orchestration.issue(request))?.config.authorizationHeader.replace(/^Bearer\s+/, "") ??
      "";
    const previewToken = (yield* preview.issue(request)).config.authorizationHeader.replace(
      /^Bearer\s+/,
      "",
    );

    expect(yield* orchestration.resolve(previewToken)).toBeUndefined();
    expect(yield* preview.resolve(orchestrationToken)).toBeUndefined();
    expect((yield* orchestration.resolve(orchestrationToken))?.threadId).toBe("root-thread");
    expect((yield* preview.resolve(previewToken))?.threadId).toBe("root-thread");
  }),
);

it.effect("does not resurrect a credential revoked during live eligibility resolution", () =>
  Effect.gen(function* () {
    let blockResolution = false;
    const entered = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    const registry = yield* makeRegistryWithEligibility(() =>
      blockResolution
        ? Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(resume)),
            Effect.as(true),
          )
        : Effect.succeed(true),
    );
    const request = {
      threadId: ThreadId.make("root-thread"),
      providerInstanceId: ProviderInstanceId.make("codex-main"),
    };
    const token =
      (yield* registry.issue(request))?.config.authorizationHeader.replace(/^Bearer\s+/, "") ?? "";

    blockResolution = true;
    const resolving = yield* registry.resolve(token).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* registry.revokeThread(request.threadId);
    yield* Deferred.succeed(resume, undefined);

    expect(yield* Fiber.join(resolving)).toBeUndefined();
    blockResolution = false;
    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect("does not issue a credential across a concurrent thread revocation", () =>
  Effect.gen(function* () {
    let eligibilityCalls = 0;
    const entered = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    const registry = yield* makeRegistryWithEligibility(() => {
      eligibilityCalls += 1;
      return eligibilityCalls === 2
        ? Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(resume)),
            Effect.as(true),
          )
        : Effect.succeed(true);
    });
    const request = {
      threadId: ThreadId.make("root-thread"),
      providerInstanceId: ProviderInstanceId.make("codex-main"),
    };

    const issuing = yield* registry.issue(request).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* registry.revokeThread(request.threadId);
    yield* Deferred.succeed(resume, undefined);

    expect(yield* Fiber.join(issuing)).toBeUndefined();
  }),
);
