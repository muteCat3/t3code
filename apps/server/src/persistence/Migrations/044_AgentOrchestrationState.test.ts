import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("044_AgentOrchestrationState", (it) => {
  it.effect("adds compatible project, thread, and session identity columns", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 43 });
      yield* runMigrations({ toMigrationInclusive: 44 });

      const projectColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_projects)
      `;
      const threadColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      const sessionColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_thread_sessions)
      `;

      assert.ok(projectColumns.some(({ name }) => name === "agent_orchestration_trusted"));
      assert.ok(threadColumns.some(({ name }) => name === "agent_parent_thread_id"));
      assert.ok(sessionColumns.some(({ name }) => name === "requested_model_selection_json"));
      assert.ok(sessionColumns.some(({ name }) => name === "applied_model_selection_json"));
      assert.ok(sessionColumns.some(({ name }) => name === "provider_reported_model_id"));
    }),
  );

  it.effect("backfills existing projects as untrusted without inventing lineage", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 43 });
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          default_thread_env_mode, favicon_path, scripts_json, created_at, updated_at, deleted_at
        ) VALUES ('project-1', 'Project', '/tmp/project', NULL, NULL, NULL, '[]',
          '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', NULL)
      `;
      yield* sql`
        INSERT INTO projection_thread_sessions (
          thread_id, status, provider_name, provider_instance_id, runtime_mode,
          active_turn_id, last_error, updated_at
        ) VALUES ('thread-legacy', 'ready', 'codex', NULL, 'full-access', NULL, NULL,
          '2026-01-01T00:00:00.000Z')
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, branch, worktree_path, linked_pull_request_json,
          latest_turn_id, created_at, updated_at, archived_at, settled_override,
          settled_at, unsettled_at, snoozed_until, snoozed_at, pinned_at,
          pin_order_key, title_regeneration_request_id, title_regeneration_started_at,
          latest_user_message_at, pending_approval_count, pending_user_input_count,
          has_actionable_proposed_plan, deleted_at
        ) VALUES (
          'thread-legacy', 'project-1', 'Legacy thread',
          '{"instanceId":"codex","model":"gpt-5-codex"}', 'full-access',
          'default', NULL, NULL, NULL, NULL,
          '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', NULL, NULL,
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, NULL
        )
      `;
      yield* runMigrations({ toMigrationInclusive: 44 });

      const rows = yield* sql<{
        readonly trusted: number;
      }>`SELECT agent_orchestration_trusted AS trusted FROM projection_projects`;
      assert.strictEqual(rows[0]?.trusted, 0);
      const threadRows = yield* sql<{ readonly parentThreadId: string | null }>`
        SELECT agent_parent_thread_id AS "parentThreadId"
        FROM projection_threads
        WHERE thread_id = 'thread-legacy'
      `;
      assert.deepStrictEqual(threadRows, [{ parentThreadId: null }]);
      const sessionRows = yield* sql<{
        readonly requested: string | null;
        readonly applied: string | null;
        readonly reported: string | null;
      }>`
        SELECT
          requested_model_selection_json AS requested,
          applied_model_selection_json AS applied,
          provider_reported_model_id AS reported
        FROM projection_thread_sessions
        WHERE thread_id = 'thread-legacy'
      `;
      assert.deepStrictEqual(sessionRows[0], {
        requested: null,
        applied: null,
        reported: null,
      });
    }),
  );
});
