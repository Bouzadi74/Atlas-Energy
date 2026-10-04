export const API_URL = "/api/atlas";

export type ScenarioRequest = {
  name: string;
  planning_year: number;
  weather_year: number;
  demand_growth: number;
  gas_price_eur_mwh_th: number;
  carbon_price_eur_t: number;
  renewable_generation_min: number;
  battery_capex_multiplier: number;
  solar_capex_multiplier: number;
  wind_capex_multiplier: number;
};

export type ScenarioPreset = {
  preset_id: string;
  request: ScenarioRequest;
  provenance: { classification: "ASSUMPTION"; version: string; note: string };
};

export type ScenarioRecord = {
  scenario_id: string;
  job_id: string;
  optimizer_run_id: string | null;
  request: ScenarioRequest;
  status: "queued" | "running" | "completed" | "failed";
  stage: string;
  processing_attempts: number;
  max_attempts: number;
  created_at: string;
  completed_at: string | null;
  failure_stage: string | null;
  error_message: string | null;
};

export type JobRecord = ScenarioRecord & { last_attempt_at: string | null };
export type ScenarioAccepted = {
  scenario_id: string;
  job_id: string;
  status: ScenarioRecord["status"];
  stage: string;
  reused: boolean;
};

export type ScenarioKpi = {
  run_id: string;
  scenario_id: string;
  scenario_name: string | null;
  planning_year: number | null;
  weather_year: number | null;
  annual_system_cost_eur: number;
  average_cost_eur_mwh: number;
  annualized_capital_cost_eur: number;
  annualized_operating_cost_eur: number;
  demand_mwh: number;
  renewable_generation_mwh: number;
  renewable_generation_share: number;
  imports_mwh: number;
  import_share: number;
  emissions_tco2: number;
  curtailment_mwh: number;
  unserved_mwh: number;
  unserved_energy_share: number;
  solve_duration_seconds: number;
  model_version: string;
  input_package_version: string | null;
};

export type CapacityResult = {
  asset: string;
  carrier: string;
  asset_role: string;
  existing_capacity_mw: number;
  new_capacity_mw: number;
  optimized_capacity_mw: number;
  energy_capacity_mwh: number | null;
};

export type CostResult = { asset: string; carrier: string; component_type: string; amount_eur: number };
export type ProvenanceRecord = { provenance_key: string; provenance_value: unknown };
export type DispatchPoint = {
  run_id: string;
  scenario_id: string;
  timestamp_utc: string;
  asset: string;
  carrier: string;
  dispatch_mw: number;
};
export type StoragePoint = {
  run_id: string;
  scenario_id: string;
  timestamp_utc: string;
  asset: string;
  carrier: string;
  net_dispatch_mw: number;
  state_of_charge_mwh: number;
};
export type ResultPage<T> = {
  items: T[];
  limit: number;
  offset: number;
  returned: number;
  has_more: boolean;
};
export type ScenarioComparison = {
  base_run_id: string;
  comparison_run_id: string;
  base_scenario_name: string | null;
  comparison_scenario_name: string | null;
  annual_cost_delta_eur: number;
  average_cost_delta_eur_mwh: number;
  renewable_share_delta: number;
  emissions_delta_tco2: number;
  curtailment_delta_mwh: number;
  unserved_delta_mwh: number;
  imports_delta_mwh: number;
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_URL}${path}`, {
      ...init,
      signal: init?.signal ?? AbortSignal.timeout(init?.method === "POST" ? 30_000 : 10_000),
      headers: { "Content-Type": "application/json", ...init?.headers },
    });
  } catch (reason) {
    if (reason instanceof Error && reason.name === "TimeoutError") {
      throw new Error("Atlas took too long to respond. Check that the API and database are running.");
    }
    throw reason;
  }
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export const atlasApi = {
  health: () => request<{ status: string; version: string; env: string }>("/health"),
  presets: () => request<ScenarioPreset[]>("/v1/scenario-presets"),
  scenarios: () => request<ScenarioRecord[]>("/v1/scenarios"),
  submitScenario: (body: ScenarioRequest) => request<ScenarioAccepted>("/v1/scenarios", { method: "POST", body: JSON.stringify(body) }),
  job: (jobId: string) => request<JobRecord>(`/v1/jobs/${jobId}`),
  kpis: (runId: string) => request<ScenarioKpi>(`/v1/results/${runId}/kpis`),
  capacity: (runId: string) => request<CapacityResult[]>(`/v1/results/${runId}/capacity`),
  costs: (runId: string) => request<CostResult[]>(`/v1/results/${runId}/costs`),
  provenance: (runId: string) => request<ProvenanceRecord[]>(`/v1/results/${runId}/provenance`),
  dispatch: (runId: string, start: string, end: string) => {
    const query = new URLSearchParams({ start, end, limit: "5000" });
    return request<ResultPage<DispatchPoint>>(`/v1/results/${runId}/dispatch?${query}`);
  },
  storage: (runId: string, start: string, end: string) => {
    const query = new URLSearchParams({ start, end, limit: "5000" });
    return request<ResultPage<StoragePoint>>(`/v1/results/${runId}/storage?${query}`);
  },
  comparisons: () => request<ScenarioComparison[]>("/v1/comparisons"),
};
