import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

interface TableInfoRow {
  readonly name: string;
}

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const projectColumns = yield* sql<TableInfoRow>`PRAGMA table_info(projection_projects)`;
  if (!projectColumns.some((column) => column.name === "agent_orchestration_trusted")) {
    yield* sql`
      ALTER TABLE projection_projects
      ADD COLUMN agent_orchestration_trusted INTEGER NOT NULL DEFAULT 0
    `;
  }

  const threadColumns = yield* sql<TableInfoRow>`PRAGMA table_info(projection_threads)`;
  if (!threadColumns.some((column) => column.name === "agent_parent_thread_id")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN agent_parent_thread_id TEXT
    `;
  }

  const sessionColumns = yield* sql<TableInfoRow>`PRAGMA table_info(projection_thread_sessions)`;
  if (!sessionColumns.some((column) => column.name === "requested_model_selection_json")) {
    yield* sql`
      ALTER TABLE projection_thread_sessions
      ADD COLUMN requested_model_selection_json TEXT
    `;
  }
  if (!sessionColumns.some((column) => column.name === "applied_model_selection_json")) {
    yield* sql`
      ALTER TABLE projection_thread_sessions
      ADD COLUMN applied_model_selection_json TEXT
    `;
  }
  if (!sessionColumns.some((column) => column.name === "provider_reported_model_id")) {
    yield* sql`
      ALTER TABLE projection_thread_sessions
      ADD COLUMN provider_reported_model_id TEXT
    `;
  }

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_threads_agent_parent
    ON projection_threads(agent_parent_thread_id, created_at, thread_id)
  `;
});
