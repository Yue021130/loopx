/** Measurement semantics for the disposable SQLite capacity entrypoint. */
import type {SqliteAuthorityBoundedProfile} from
  "../../loopx/control_plane/coordination/sqlite_authority_store.ts";

export interface Latency {
  n: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
}

export function latency(samples: readonly number[]): Latency {
  if (!samples.length || samples.some(value => !Number.isFinite(value) || value < 0)) {
    throw new Error("latency requires nonempty finite nonnegative samples");
  }
  const sorted = [...samples].sort((left, right) => left - right);
  const at = (p: number) => sorted[Math.ceil(sorted.length * p) - 1]!;
  return {n: sorted.length, p50_ms: at(.5), p95_ms: at(.95), p99_ms: at(.99)};
}

export interface CapacityAxis {
  target_commits: number;
  completed_commits: number;
  projection_json_bytes: number;
  sample_window: number;
  status: "passed" | "failed";
  warm: Record<"commit" | "head" | "receipt" | "scan_100", Latency> | null;
  cold_node: Latency | null;
  cold_cli: Record<"mutation" | "status" | "quota", Latency> | null;
  application_request_json_bytes: number;
  files_at_target: {database_bytes: number; wal_bytes: number; shm_bytes: number} | null;
  /** Retained-state profile of the filled history; null when it was unavailable. */
  bounded_profile: SqliteAuthorityBoundedProfile | null;
  /** Linear archive audit, only requested where its cost is affordable. */
  history_audit: {status: string; commits: number; checkpoints: number} | null;
  sampled_peak_rss_bytes: number;
  resource_peak_rss_bytes: number;
  fill_seconds: number;
  cli_commits: number;
  cleanup_verified: boolean;
  failure?: string;
}

export interface QualificationRow {
  id: string;
  status: "passed" | "failed" | "missing";
  scope: string;
  observed?: number;
  budget?: number;
  unit?: "ms" | "ratio" | "delta_ms" | "bytes";
}

/** Thresholds come from RFC 7.2; a rehearsal cannot qualify the full profile. */
export function capacityLedger(axes: readonly CapacityAxis[], formal: boolean): QualificationRow[] {
  const rows: QualificationRow[] = [];
  const valid = (value: Latency | undefined, samples: number): boolean => !!value && value.n === samples &&
    [value.p50_ms, value.p95_ms, value.p99_ms].every(n => Number.isFinite(n) && n >= 0) &&
    value.p50_ms <= value.p95_ms && value.p95_ms <= value.p99_ms;
  const baseline = axes.find(axis => axis.target_commits === 10000);
  const final = axes.find(axis => axis.target_commits === 100000);
  const ready = formal && axes.length === 2 && baseline?.status === "passed" && final?.status === "passed" &&
    [baseline, final].every(axis => axis.completed_commits === axis.target_commits &&
      axis.projection_json_bytes === 65536 && axis.sample_window === 1000 && axis.cleanup_verified &&
      valid(axis.warm?.commit, 1000) && valid(axis.warm?.head, 3000) &&
      valid(axis.warm?.receipt, 2000) && valid(axis.warm?.scan_100, 200));
  rows.push({id: "matched_profile_execution", status: axes.some(axis => axis.status === "failed") ? "failed" :
    ready ? "passed" : "missing", scope: "complete 64 KiB 10k/100k runs and declared sample counts"});
  const add = (id: string, value: number | undefined, budget: number,
    unit: "ms" | "ratio" | "delta_ms" | "bytes") => {
    if (!ready || value === undefined || !Number.isFinite(value) || (value < 0 && unit !== "delta_ms")) {
      rows.push({id, status: "missing", scope: "requires the complete matched 64 KiB 10k/100k profile"});
    } else rows.push({id, status: value <= budget ? "passed" : "failed", scope: "fixed 64 KiB storage axis",
      observed: value, budget, unit});
  };
  for (const [key, budget] of [["commit", 100], ["head", 50], ["receipt", 50], ["scan_100", 250]] as const) {
    add(`${key}_p95`, final?.warm?.[key].p95_ms, budget, "ms");
  }
  for (const key of ["commit", "head", "receipt"] as const) {
    const denominator = baseline?.warm?.[key].p95_ms;
    add(`${key}_history_growth`, denominator && final?.warm ? final.warm[key].p95_ms / denominator : undefined, 2, "ratio");
  }
  add("cold_cli_status_p95", valid(final?.cold_cli?.status, 20) ? final?.cold_cli?.status.p95_ms : undefined, 2000, "ms");
  add("cold_cli_mutation_increment_p95", valid(baseline?.cold_cli?.mutation, 20) && valid(final?.cold_cli?.mutation, 20) && baseline?.cold_cli && final?.cold_cli
    ? final.cold_cli.mutation.p95_ms - baseline.cold_cli.mutation.p95_ms : undefined, 200, "delta_ms");
  // Retained state must be bounded by checkpoint windows rather than by how
  // many transactions the history holds. These rows read the provider's own
  // profile, so a provider that keeps one full copy per commit cannot pass.
  const profile = final?.bounded_profile;
  const profileReady = ready && profile !== null && profile !== undefined &&
    final !== undefined && profile.cursor === String(final.completed_commits) &&
    profile.commits === final.completed_commits;
  const checkpointBound = (axis: CapacityAxis | undefined): boolean => {
    const value = axis?.bounded_profile;
    return !!value && value.checkpoint_interval === 64 &&
      value.checkpoints === Math.ceil(value.commits / value.checkpoint_interval) &&
      value.recovery_tail_commits <= value.replay_budget_commits;
  };
  if (!profileReady || !checkpointBound(baseline) || !checkpointBound(final) || profile === null ||
    profile === undefined || final === undefined) {
    rows.push({id: "bounded_retained_state", status: "missing",
      scope: "requires the complete matched profile and a bounded checkpoint profile per axis"});
  } else {
    const perCommitCopy = final.projection_json_bytes * final.completed_commits;
    const retained = profile.retained_projection_bytes + profile.retained_delta_bytes;
    rows.push({id: "bounded_retained_state", status: retained <= perCommitCopy / 8 ? "passed" : "failed",
      scope: "retained checkpoint and delta bytes against one full copy per retained commit",
      observed: retained, budget: Math.floor(perCommitCopy / 8), unit: "bytes"});
  }
  const scope: Record<string, string> = {
    domain_workload: "eight agents, four writers, leases/capture/archive and the production-scale fixture remain separate",
    cumulative_storage_writes: "application input bytes and final files cannot qualify logical writes, WAL traffic or the <=15x budget",
    lock_wait_distribution: "no pure busy-handler timing is exposed by this node:sqlite driver",
    steady_state_rss: "sampled RSS and per-process peak are observations, not a proof across steady-state windows",
    large_history_recovery: "small fault regressions do not qualify bounded recovery of a 100k history; the linear archive audit is only launched in the rehearsal profile",
    payload_and_headroom: "1 MiB, 300k and bursts are not launched by this profile",
    consumer_lag: "24-hour logical consumer backlog requires its own persisted-cursor test",
    restore_upgrade_rollback: "fenced restore lineage and supported upgrade/rollback are not implemented by this harness",
    elapsed_soak: "at least ten actual days require a separately authorized recoverable synthetic soak",
    os_runtime_matrix: "one local run cannot qualify every supported OS and installed runtime",
    promotion: "provider defaults, live migration and D3 remain separately gated",
  };
  for (const [id, reason] of Object.entries(scope)) rows.push({id, status: "missing", scope: reason});
  return rows;
}
