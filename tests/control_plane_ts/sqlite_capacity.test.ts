import assert from "node:assert/strict";
import {spawnSync} from "node:child_process";
import {mkdtemp, readFile, rm} from "node:fs/promises";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
import test from "node:test";
import {capacityLedger, latency, type CapacityAxis} from "../../examples/coordination/sqlite-capacity-report.ts";

test("latency has explicit nearest-rank tails and rejects absent or invalid samples", () => {
  assert.deepEqual(latency([5, 1, 4, 2, 3]), {n: 5, p50_ms: 3, p95_ms: 5, p99_ms: 5});
  for (const samples of [[], [NaN], [Infinity], [-1]]) assert.throws(() => latency(samples));
});

function axis(count: number): CapacityAxis {
  const sample = (n: number) => ({n, p50_ms: 1, p95_ms: 2, p99_ms: 3});
  return {target_commits: count, completed_commits: count, projection_json_bytes: 65536,
    sample_window: 1000, status: "passed", cleanup_verified: true,
    warm: {commit: sample(1000), head: sample(3000), receipt: sample(2000), scan_100: sample(200)},
    cold_node: sample(20), cold_cli: {mutation: sample(20), status: sample(20), quota: sample(20)},
    bounded_profile: {schema_version: "loopx_sqlite_authority_bounded_profile_v0", status: "available",
      cursor: String(count), commits: count, checkpoints: Math.ceil(count / 64), checkpoint_interval: 64,
      replay_budget_commits: 63, recovery_tail_commits: 0, retained_projection_bytes: 1024,
      retained_delta_bytes: 1024, retained_payload_bytes: 0, database_bytes: 4096, wal_bytes: 0, shm_bytes: 0},
    history_audit: {status: "verified", commits: count, checkpoints: Math.ceil(count / 64)},
    application_request_json_bytes: 0, files_at_target: {database_bytes: 0, wal_bytes: 0, shm_bytes: 0},
    sampled_peak_rss_bytes: 0, resource_peak_rss_bytes: 0, fill_seconds: 0, cli_commits: 20};
}

test("budget failure remains failed; small rehearsals and unavailable metrics stay missing", () => {
  const baseline = axis(10000), final = axis(100000);
  final.warm!.head = {n: 3000, p50_ms: 1, p95_ms: 5, p99_ms: 6};
  const rows = capacityLedger([baseline, final], true);
  assert.equal(rows.find(row => row.id === "head_history_growth")?.status, "failed");
  assert.equal(rows.find(row => row.id === "head_p95")?.status, "passed");
  assert.equal(rows.find(row => row.id === "cumulative_storage_writes")?.status, "missing");
  assert.equal(rows.find(row => row.id === "elapsed_soak")?.status, "missing");
  assert(capacityLedger([baseline, final], false).every(row => row.status === "missing"));
  final.status = "failed";
  assert.equal(capacityLedger([baseline, final], true)[0]?.status, "failed");
});

test("incomplete, wrong-size or malformed evidence cannot satisfy matched budgets", () => {
  for (const change of [
    (value: CapacityAxis) => {value.completed_commits--;},
    (value: CapacityAxis) => {value.projection_json_bytes = 4096;},
    (value: CapacityAxis) => {value.warm!.head.n = 0;},
    (value: CapacityAxis) => {value.warm!.head.p95_ms = NaN;},
    (value: CapacityAxis) => {value.warm!.head.p50_ms = 99;},
    (value: CapacityAxis) => {value.cleanup_verified = false;},
  ]) {
    const final = axis(100000); change(final);
    assert.equal(capacityLedger([axis(10000), final], true).find(row => row.id === "head_p95")?.status, "missing");
  }
  const final = axis(100000); final.cold_cli = null;
  const rows = capacityLedger([axis(10000), final], true);
  assert.equal(rows.find(row => row.id === "head_p95")?.status, "passed");
  assert.equal(rows.find(row => row.id === "cold_cli_status_p95")?.status, "missing");
});

test("CLI latency improvement is retained as a signed difference", () => {
  const baseline = axis(10000), final = axis(100000);
  baseline.cold_cli!.mutation = {n: 20, p50_ms: 1, p95_ms: 8, p99_ms: 9};
  const row = capacityLedger([baseline, final], true).find(item => item.id === "cold_cli_mutation_increment_p95");
  assert.equal(row?.observed, -6);
  assert.equal(row?.status, "passed");
});

test("small capacity entrypoint exercises real SQLite and never claims a full qualification", {timeout: 60000}, async t => {
  const directory = await mkdtemp(join(tmpdir(), "sqlite-capacity-report-"));
  t.after(() => rm(directory, {recursive: true, force: true}));
  const output = join(directory, "report.json");
  const child = spawnSync(process.execPath, ["--no-warnings", "--experimental-sqlite", "--experimental-strip-types",
    fileURLToPath(new URL("../../examples/coordination/sqlite-capacity.ts", import.meta.url)),
    "--profile", "rehearsal", "--output", output], {encoding: "utf8", timeout: 55000});
  assert.equal(child.status, 0, child.stderr);
  const report = JSON.parse(await readFile(output, "utf8"));
  assert.equal(report.full_d2_qualified, false);
  assert.deepEqual(report.axes.map((row: CapacityAxis) => row.completed_commits), [100, 1000]);
  assert(report.axes.every((row: CapacityAxis) => row.cleanup_verified && row.status === "passed"));
  assert(report.ledger.every((row: {status: string}) => row.status === "missing"));
  assert.equal(report.metric_limits.cumulative_wal_traffic, "missing");
  assert.equal(report.workload.cold_cli, "not_requested");
  assert.equal(report.runtime.sqlite_version.length > 0, true);
});
