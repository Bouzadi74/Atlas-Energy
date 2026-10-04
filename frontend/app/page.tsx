"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { Activity, ArrowUpRight, ChartNoAxesCombined, Check, CircleAlert, Database, GitCompareArrows, LayoutDashboard, Radar, RefreshCw, SlidersHorizontal, type LucideIcon } from "lucide-react";
import {
  API_URL,
  atlasApi,
  CapacityResult,
  CostResult,
  DispatchPoint,
  JobRecord,
  ProvenanceRecord,
  ScenarioComparison,
  ScenarioKpi,
  ScenarioPreset,
  ScenarioRecord,
  ScenarioRequest,
  StoragePoint,
} from "@/lib/atlas-api";

type View = "overview" | "builder" | "operations" | "compare" | "transparency";

const EMPTY_SCENARIO: ScenarioRequest = {
  name: "Custom scenario", planning_year: 2030, weather_year: 2024,
  demand_growth: 0.03, gas_price_eur_mwh_th: 65, carbon_price_eur_t: 50,
  renewable_generation_min: 0.5, battery_capex_multiplier: 1,
  solar_capex_multiplier: 1, wind_capex_multiplier: 1,
};
const STAGE_LABELS: Record<string, string> = {
  queued: "Waiting to start", optimizing: "Calculating the scenario", optimized: "Calculation finished",
  publishing: "Saving the results", published: "Results saved",
  building_analytics: "Preparing the charts", analytics_ready: "Charts are ready",
  completed: "Ready to view", failed: "Could not finish",
};
const CARRIER_COLORS: Record<string, string> = {
  solar: "#e8bc67", wind: "#b9e65a", hydro: "#63b9c6", gas: "#f2785c",
  coal: "#697d79", oil: "#b88c6b", battery: "#84a8e2", import: "#d8a981",
};
const COST_COLORS = ["#f2785c", "#e8bc67", "#b9e65a", "#63b9c6", "#84a8e2", "#8ca49a"];
const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const decimal = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
const money = (value: number) => `${value < 0 ? "−" : ""}€${compact.format(Math.abs(value))}`;
const moneyPerMwh = (value: number) => `${value < 0 ? "−" : ""}€${decimal.format(Math.abs(value))}/MWh`;
const pct = (value: number) => `${(value * 100).toFixed(1)}%`;

export default function Home() {
  const [view, setView] = useState<View>("overview");
  const [online, setOnline] = useState(false);
  const [presets, setPresets] = useState<ScenarioPreset[]>([]);
  const [scenarios, setScenarios] = useState<ScenarioRecord[]>([]);
  const [comparisons, setComparisons] = useState<ScenarioComparison[]>([]);
  const [activeRun, setActiveRun] = useState("");
  const [kpi, setKpi] = useState<ScenarioKpi | null>(null);
  const [capacity, setCapacity] = useState<CapacityResult[]>([]);
  const [costs, setCosts] = useState<CostResult[]>([]);
  const [provenance, setProvenance] = useState<ProvenanceRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [resultLoading, setResultLoading] = useState(false);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      const [health, nextPresets, nextScenarios, nextComparisons] = await Promise.all([
        atlasApi.health(), atlasApi.presets(), atlasApi.scenarios(), atlasApi.comparisons(),
      ]);
      setOnline(health.status === "ok"); setPresets(nextPresets);
      setScenarios(nextScenarios); setComparisons(nextComparisons);
      setActiveRun((current) => current || nextScenarios.filter((item) => item.status === "completed" && item.optimizer_run_id).at(-1)?.optimizer_run_id || "");
      setError("");
    } catch (reason) {
      setOnline(false); setError(reason instanceof Error ? reason.message : "Could not connect to Atlas API");
    } finally { setLoading(false); }
  }, []);

  useEffect(() => void refresh(), [refresh]);
  useEffect(() => {
    if (!activeRun) { setKpi(null); setCapacity([]); setCosts([]); setProvenance([]); return; }
    let cancelled = false;
    setResultLoading(true); setKpi(null); setCapacity([]); setCosts([]); setProvenance([]);
    Promise.all([atlasApi.kpis(activeRun), atlasApi.capacity(activeRun), atlasApi.costs(activeRun), atlasApi.provenance(activeRun)])
      .then(([nextKpi, nextCapacity, nextCosts, nextProvenance]) => {
        if (!cancelled) { setKpi(nextKpi); setCapacity(nextCapacity); setCosts(nextCosts); setProvenance(nextProvenance); }
      })
      .catch((reason) => !cancelled && setError(reason instanceof Error ? reason.message : String(reason)))
      .finally(() => { if (!cancelled) setResultLoading(false); });
    return () => { cancelled = true; };
  }, [activeRun]);

  const completed = useMemo(() => scenarios.filter((item) => item.status === "completed" && item.optimizer_run_id), [scenarios]);

  return <div className="app-shell">
    <aside className="sidebar">
      <button className="brand" onClick={() => setView("overview")} aria-label="Atlas Energy home"><span className="brand-mark"><Activity size={23} strokeWidth={1.8} aria-hidden="true" /></span><span><b>ATLAS</b><small>ENERGY SYSTEMS</small></span></button>
      <p className="nav-heading">EXPLORE</p>
      <nav aria-label="Primary navigation">
        <NavButton active={view === "overview"} icon={LayoutDashboard} label="Results" shortLabel="Results" onClick={() => setView("overview")} />
        <NavButton active={view === "builder"} icon={SlidersHorizontal} label="Try a scenario" shortLabel="Try" onClick={() => setView("builder")} />
        <NavButton active={view === "operations"} icon={ChartNoAxesCombined} label="Hour by hour" shortLabel="Hours" onClick={() => setView("operations")} />
        <NavButton active={view === "compare"} icon={GitCompareArrows} label="Compare plans" shortLabel="Compare" onClick={() => setView("compare")} />
        <NavButton active={view === "transparency"} icon={Database} label="Data & limits" shortLabel="Sources" onClick={() => setView("transparency")} />
      </nav>
      <div className="sidebar-context"><span>ABOUT THIS TOOL</span><b>Morocco-wide estimates</b><small>For learning and comparison</small></div>
      <div className="sidebar-foot"><span className={`status-dot ${online ? "online" : ""}`} /><div><b>{online ? "Connected" : "Not connected"}</b><small title={API_URL}>Atlas data service</small></div></div>
    </aside>
    <main className="workspace">
      <header className="topbar"><div className="topbar-location"><span className="region-mark">MA</span><span><b>Morocco</b><small>Electricity planning explorer</small></span></div><div className="topbar-meta"><span>Weather: 2024</span><span>One year, hour by hour</span><span className={`topbar-health ${online ? "online" : ""}`}>{online ? "CONNECTED" : "NOT CONNECTED"}</span></div></header>
      {error && <div className="error-banner" role="alert"><CircleAlert size={18} aria-hidden="true" /><span><b>Atlas couldn't load the data.</b> If you're running it locally, check the API and database, then try again.<details className="error-details"><summary>Technical details</summary>{humanError(error)}</details></span><button onClick={() => void refresh()}><RefreshCw size={15} aria-hidden="true" /> Try again</button></div>}
      <div key={view} className="view-stage" id="main-content">
        {loading && <LoadingScreen />}
        {!loading && view === "overview" && <ExecutiveOverview online={online} onRetry={refresh} completed={completed} activeRun={activeRun} setActiveRun={setActiveRun} resultLoading={resultLoading} kpi={kpi} capacity={capacity} costs={costs} setView={setView} />}
        {!loading && view === "builder" && <ScenarioBuilder online={online} presets={presets} scenarios={scenarios} onRefresh={refresh} onOpenDashboard={(runId) => { setActiveRun(runId); setView("overview"); }} />}
        {!loading && view === "operations" && <OperationsExplorer online={online} completed={completed} initialRun={activeRun} />}
        {!loading && view === "compare" && <ComparisonView online={online} completed={completed} comparisons={comparisons} />}
        {!loading && view === "transparency" && <TransparencyView kpi={kpi} provenance={provenance} presets={presets} />}
      </div>
    </main>
  </div>;
}

function NavButton({ active, icon: Icon, label, shortLabel, onClick }: { active: boolean; icon: LucideIcon; label: string; shortLabel: string; onClick: () => void }) {
  return <button className={active ? "active" : ""} onClick={onClick} aria-label={label} aria-current={active ? "page" : undefined}><Icon size={19} strokeWidth={1.8} aria-hidden="true" /><span className="nav-label">{label}</span><span className="nav-label-mobile">{shortLabel}</span></button>;
}
function LoadingScreen() { return <div className="loading-screen"><span className="loader" /><p>Loading scenarios…</p></div>; }
function PageIntro({ eyebrow, title, text, action }: { eyebrow: string; title: string; text: string; action?: React.ReactNode }) {
  return <div className="page-intro"><div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p>{text}</p></div>{action}</div>;
}

function ExecutiveOverview({ online, onRetry, completed, activeRun, setActiveRun, resultLoading, kpi, capacity, costs, setView }: {
  online: boolean; onRetry: () => Promise<void>; completed: ScenarioRecord[]; activeRun: string; setActiveRun: (id: string) => void; resultLoading: boolean; kpi: ScenarioKpi | null;
  capacity: CapacityResult[]; costs: CostResult[]; setView: (view: View) => void;
}) {
  const rows = capacity
    .filter((row) => row.optimized_capacity_mw > 0 && row.asset_role !== "reliability_slack" && row.carrier !== "imports")
    .sort((a, b) => b.optimized_capacity_mw - a.optimized_capacity_mw);
  const maxCapacity = Math.max(...rows.map((row) => row.optimized_capacity_mw), 1);
  const costGroups = aggregateCosts(costs); const totalCost = costGroups.reduce((sum, item) => sum + item.value, 0) || 1;
  return <>
    <PageIntro eyebrow="01 / RESULTS" title="Explore Morocco's electricity choices." text="See what the model estimates for cost, power sources and emissions under different plans. These are possible futures, not live grid data." action={online && <button className="primary-button" onClick={() => setView("builder")}>Try your own plan <ArrowUpRight size={18} aria-hidden="true" /></button>} />
    <section className="start-guide" aria-label="How to use this page"><div><span>NEW HERE?</span><h2>Start with a scenario.</h2><p>A scenario is a set of choices about a future year. Pick one below, read its results, then compare it with another plan.</p></div><ol><li><b>1</b> Pick a plan</li><li><b>2</b> Read the estimates</li><li><b>3</b> Compare the trade-offs</li></ol></section>
    {!online && completed.length === 0 ? <section className="empty-state offline-state"><Radar size={34} aria-hidden="true" /><h2>Results aren't available right now</h2><p>Atlas cannot reach its data service. If you're running the project locally, start the API and database, then try again. You cannot run a new scenario while disconnected.</p><button className="secondary-button" onClick={() => void onRetry()}><RefreshCw size={16} aria-hidden="true" /> Try again</button></section> : <>
    <div className="run-toolbar"><label>Choose a scenario<select value={activeRun} onChange={(event) => setActiveRun(event.target.value)}>{completed.map((scenario) => <option key={scenario.optimizer_run_id!} value={scenario.optimizer_run_id!}>{scenario.request.name}</option>)}</select></label><span className="result-badge"><i /> {completed.length} ready to explore</span></div>
    {resultLoading ? <section className="panel chart-loading" aria-live="polite"><span className="loader" /><p>Loading this scenario's results…</p></section> : !kpi ? <EmptyState title={completed.length ? "This result couldn't be loaded" : "No results to show yet"} text={completed.length ? "Try again in a moment or choose another scenario." : "Run a scenario to see its estimates here. Keep the background services running while Atlas calculates it."} action={completed.length ? undefined : () => setView("builder")} /> : <>
      <section className="kpi-grid">
        <KpiCard label="Estimated yearly cost" value={money(kpi.annual_system_cost_eur)} detail={`${money(kpi.average_cost_eur_mwh)} per MWh of demand`} tone="sand" />
        <KpiCard label="Electricity from renewables" value={pct(kpi.renewable_generation_share)} detail={`${compact.format(kpi.renewable_generation_mwh)} MWh generated`} tone="green" />
        <KpiCard label="Estimated CO₂ emissions" value={`${compact.format(kpi.emissions_tco2)} t`} detail="From this modeled electricity system" tone="red" />
        <KpiCard label="Electricity demand not met" value={pct(kpi.unserved_energy_share)} detail={`${decimal.format(kpi.unserved_mwh)} MWh shortfall`} tone={kpi.unserved_mwh > 1 ? "red" : "blue"} />
      </section>
      <p className="unit-note"><b>Reading the units:</b> MW is power available at one moment; MWh is electricity produced or used over time. Costs are estimates for the whole modeled system, not a household bill.</p>
      <div className="two-column">
        <section className="panel capacity-panel"><PanelHead title="How much power is installed?" subtitle="Power plants and storage, measured in MW" tag={`${kpi.planning_year}`} />
          <div className="bar-chart">{rows.map((row) => <div className="bar-row" key={`${row.asset}-${row.asset_role}`}><div className="bar-label"><b>{friendly(row.asset)}</b><span>{compact.format(row.optimized_capacity_mw)} MW</span></div><div className="bar-track"><span className="bar-existing" style={{ width: `${row.existing_capacity_mw / maxCapacity * 100}%`, background: carrierColor(row.carrier) }} /><span className="bar-new" style={{ width: `${row.new_capacity_mw / maxCapacity * 100}%`, background: carrierColor(row.carrier) }} /></div></div>)}</div>
          <div className="legend"><span><i className="existing" /> Already built</span><span><i className="new" /> Added in this plan</span></div>
        </section>
        <section className="panel cost-panel"><PanelHead title="What makes up the cost?" subtitle="Estimated cost for one year" tag={money(kpi.annual_system_cost_eur)} />
          <div className="cost-visual"><div className="donut" style={{ background: donutGradient(costGroups, totalCost) }}><div><b>{money(totalCost)}</b><span>annual</span></div></div><div className="cost-list">{costGroups.map((item, index) => <div key={item.label}><i style={{ background: COST_COLORS[index % COST_COLORS.length] }} /><span>{friendly(item.label)}</span><b>{pct(item.value / totalCost)}</b></div>)}</div></div>
          <p className="model-note">Includes building and running plants, fuel, carbon, imports and any electricity shortfall. This is a system estimate, not a household bill.</p>
        </section>
      </div>
      <section className="insight-strip"><div><span className="insight-icon">↗</span><p><b>Imports cover {pct(kpi.import_share)} of estimated electricity use.</b><br />Total annual demand in this plan is {compact.format(kpi.demand_mwh)} MWh.</p></div><button onClick={() => setView("compare")}>Compare with another plan →</button></section>
    </>}
    </>}
  </>;
}

function KpiCard({ label, value, detail, tone }: { label: string; value: string; detail: string; tone: string }) { return <article className={`kpi-card ${tone}`}><span>{label}</span><b>{value}</b><small>{detail}</small></article>; }
function PanelHead({ title, subtitle, tag }: { title: string; subtitle: string; tag: string }) { return <div className="panel-head"><div><h2>{title}</h2><p>{subtitle}</p></div><span>{tag}</span></div>; }
function EmptyState({ title, text, action }: { title: string; text: string; action?: () => void }) { return <section className="empty-state"><Radar size={31} strokeWidth={1.8} aria-hidden="true" /><h2>{title}</h2><p>{text}</p>{action && <button className="primary-button" onClick={action}>Try a scenario</button>}</section>; }

function ScenarioBuilder({ online, presets, scenarios, onRefresh, onOpenDashboard }: { online: boolean; presets: ScenarioPreset[]; scenarios: ScenarioRecord[]; onRefresh: () => Promise<void>; onOpenDashboard: (runId: string) => void }) {
  const defaultPreset = presets.find((preset) => preset.preset_id === "baseline-2030") ?? presets[0];
  const [selected, setSelected] = useState(defaultPreset?.preset_id ?? "custom");
  const [form, setForm] = useState<ScenarioRequest>(defaultPreset?.request ?? EMPTY_SCENARIO);
  const [job, setJob] = useState<JobRecord | null>(null); const [submitting, setSubmitting] = useState(false); const [submitError, setSubmitError] = useState("");
  const selectedPreset = presets.find((preset) => preset.preset_id === selected);
  function choosePreset(id: string) { setSelected(id); const preset = presets.find((item) => item.preset_id === id); if (preset) setForm({ ...preset.request }); }
  async function submit(event: FormEvent) {
    event.preventDefault(); setSubmitting(true); setSubmitError(""); setJob(null);
    try { const accepted = await atlasApi.submitScenario(form); setJob(await atlasApi.job(accepted.job_id)); await onRefresh(); }
    catch (reason) { setSubmitError(humanError(reason instanceof Error ? reason.message : String(reason))); }
    finally { setSubmitting(false); }
  }
  useEffect(() => {
    if (!job || ["completed", "failed"].includes(job.status)) return;
    const timer = window.setInterval(async () => { try { const next = await atlasApi.job(job.job_id); setJob(next); if (["completed", "failed"].includes(next.status)) await onRefresh(); } catch { /* retain persisted state */ } }, 2500);
    return () => window.clearInterval(timer);
  }, [job, onRefresh]);
  const recent = [...scenarios].reverse().slice(0, 6);
  return <>
    <PageIntro eyebrow="02 / TRY A SCENARIO" title="What if we changed the plan?" text="Choose an example, change any assumptions you like, then run it. The calculation can take several minutes." />
    <div className="builder-layout"><form className="panel builder-form" onSubmit={submit}>
      <div className="form-section"><div className="section-number">01</div><div><h2>Pick an example to start with</h2><p>Choose one below, or start from scratch. You can change every number.</p></div></div>
      <div className="preset-grid">{presets.map((preset) => <button type="button" key={preset.preset_id} className={selected === preset.preset_id ? "selected" : ""} aria-pressed={selected === preset.preset_id} onClick={() => choosePreset(preset.preset_id)}><b>{preset.request.name}</b><span>Plan for {preset.request.planning_year}</span></button>)}<button type="button" className={selected === "custom" ? "selected" : ""} aria-pressed={selected === "custom"} onClick={() => { setSelected("custom"); setForm({ ...EMPTY_SCENARIO }); }}><b>Start from scratch</b><span>Make your own choices</span></button></div>
      {selectedPreset && <details className="preset-note"><summary>About this example</summary><p>{selectedPreset.provenance.note}</p></details>}
      <div className="form-section"><div className="section-number">02</div><div><h2>Adjust your assumptions</h2><p>These are inputs to a model, not predictions of what will happen.</p></div></div>
      <div className="field-grid">
        <Field label="Name this scenario" wide><input required minLength={3} maxLength={120} value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} /></Field>
        <Field label="Year to plan for"><input required type="number" min="2024" max="2100" value={form.planning_year} onChange={(event) => setForm({ ...form, planning_year: +event.target.value })} /></Field>
        <Field label="Growth in electricity use each year" suffix="%" help="For example, 3 means electricity use grows by 3% per year."><input required type="number" step="0.1" min="-20" max="30" value={form.demand_growth * 100} onChange={(event) => setForm({ ...form, demand_growth: +event.target.value / 100 })} /></Field>
        <Field label="Price of gas fuel" suffix="€/MWhₜₕ" help="Cost per unit of heat energy from gas."><input required type="number" step="1" min="0" value={form.gas_price_eur_mwh_th} onChange={(event) => setForm({ ...form, gas_price_eur_mwh_th: +event.target.value })} /></Field>
        <Field label="Price placed on CO₂ emissions" suffix="€/tCO₂"><input required type="number" step="1" min="0" value={form.carbon_price_eur_t} onChange={(event) => setForm({ ...form, carbon_price_eur_t: +event.target.value })} /></Field>
        <Field label="Minimum electricity from renewables" suffix="%" help="The smallest share of annual generation allowed from renewable sources."><input required type="number" step="1" min="0" max="100" value={form.renewable_generation_min * 100} onChange={(event) => setForm({ ...form, renewable_generation_min: +event.target.value / 100 })} /></Field>
        <Field label="Solar building cost" suffix="×"><input required type="number" step="0.05" min="0.05" max="5" value={form.solar_capex_multiplier} onChange={(event) => setForm({ ...form, solar_capex_multiplier: +event.target.value })} /></Field>
        <Field label="Wind building cost" suffix="×"><input required type="number" step="0.05" min="0.05" max="5" value={form.wind_capex_multiplier} onChange={(event) => setForm({ ...form, wind_capex_multiplier: +event.target.value })} /></Field>
        <Field label="Battery building cost" suffix="×"><input required type="number" step="0.05" min="0.05" max="5" value={form.battery_capex_multiplier} onChange={(event) => setForm({ ...form, battery_capex_multiplier: +event.target.value })} /></Field>
      </div>
      <p className="field-explainer">For building costs, <b>1×</b> uses the reference estimate; <b>0.8×</b> makes it 20% lower.</p>
      {submitError && <p className="inline-error" role="alert">{submitError}</p>}
      {!online && <p className="inline-error" role="status">Atlas is not connected. Start the data service before running this scenario.</p>}
      <div className="submit-row"><p>Uses weather from <b>{form.weather_year}</b>. Repeating the same choices reuses the saved calculation.</p><button className="primary-button" disabled={!online || submitting}>{submitting ? "Starting…" : "Run this scenario →"}</button></div>
    </form><aside className="builder-side"><section className="panel job-card"><h2>Run progress</h2>{job ? <JobStatus job={job} onOpen={() => job.optimizer_run_id && onOpenDashboard(job.optimizer_run_id)} /> : <div className="job-empty"><Activity size={29} strokeWidth={1.6} aria-hidden="true" /><p>After you run a scenario, follow its progress here.</p></div>}</section><section className="panel recent-runs"><h2>Recent scenarios</h2>{recent.length ? recent.map((item) => <div key={item.job_id}><span className={`run-status ${item.status}`} /><p><b>{item.request.name}</b><small>{STAGE_LABELS[item.stage] ?? item.stage}</small></p><time>{new Date(item.created_at).toLocaleDateString()}</time></div>) : <p className="muted">Nothing run yet.</p>}</section></aside></div>
  </>;
}

function Field({ label, suffix, help, wide, children }: { label: string; suffix?: string; help?: string; wide?: boolean; children: React.ReactNode }) { return <label className={wide ? "wide" : ""}><span>{label}</span><div>{children}{suffix && <small>{suffix}</small>}</div>{help && <em className="field-help">{help}</em>}</label>; }
function JobStatus({ job, onOpen }: { job: JobRecord; onOpen: () => void }) {
  const steps = ["queued", "optimizing", "publishing", "building_analytics", "completed"];
  const progress: Record<string, number> = {
    queued: 0, optimizing: 1, optimized: 1, publishing: 2, published: 2,
    building_analytics: 3, analytics_ready: 3, completed: 4,
  };
  const stageIndex = job.status === "failed" ? -1 : (progress[job.stage] ?? 0);
  return <div className="job-status"><div className="job-title"><span className={`pulse ${job.status}`} /><div><b>{STAGE_LABELS[job.stage] ?? job.stage}</b><small>Run ID: {job.job_id}</small></div></div><div className="stepper">{steps.map((step, index) => <div className={index <= stageIndex ? "done" : ""} key={step}><i>{index < stageIndex ? <Check size={13} aria-hidden="true" /> : index + 1}</i><span>{STAGE_LABELS[step]}</span></div>)}</div>{job.status === "failed" && <p className="inline-error">This run stopped during {job.failure_stage ?? "processing"}. {job.error_message}</p>}{job.status === "completed" && <button className="secondary-button" onClick={onOpen}>See results →</button>}<p className="job-meta">Attempt {job.processing_attempts} of {job.max_attempts}</p></div>;
}

type HourlySeries = { name: string; color: string; values: number[] };

function OperationsExplorer({ online, completed, initialRun }: { online: boolean; completed: ScenarioRecord[]; initialRun: string }) {
  const initialScenario = completed.find((item) => item.optimizer_run_id === initialRun) ?? completed[0];
  const [runId, setRunId] = useState(initialScenario?.optimizer_run_id ?? "");
  const [date, setDate] = useState(`${initialScenario?.request.planning_year ?? 2030}-01-01`);
  const [dispatch, setDispatch] = useState<DispatchPoint[]>([]);
  const [storage, setStorage] = useState<StoragePoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const scenario = completed.find((item) => item.optimizer_run_id === runId);
  const planningYear = scenario?.request.planning_year ?? 2030;

  function chooseRun(nextRun: string) {
    const nextScenario = completed.find((item) => item.optimizer_run_id === nextRun);
    setRunId(nextRun);
    setDate(`${nextScenario?.request.planning_year ?? 2030}-01-01`);
  }

  useEffect(() => {
    if (!runId || !date) return;
    let cancelled = false;
    setLoading(true); setError("");
    const start = `${date}T00:00:00Z`;
    const end = `${shiftDate(date, 1)}T00:00:00Z`;
    Promise.all([atlasApi.dispatch(runId, start, end), atlasApi.storage(runId, start, end)])
      .then(([dispatchPage, storagePage]) => {
        if (cancelled) return;
        if (dispatchPage.has_more || storagePage.has_more) throw new Error("Hourly result exceeded the safe page limit");
        setDispatch(dispatchPage.items); setStorage(storagePage.items);
      })
      .catch((reason) => !cancelled && setError(humanError(reason instanceof Error ? reason.message : String(reason))))
      .finally(() => !cancelled && setLoading(false));
    return () => { cancelled = true; };
  }, [runId, date]);

  const hourly = useMemo(() => aggregateDispatch(dispatch), [dispatch]);
  const storageSeries = useMemo(() => aggregateStorage(storage, hourly.times), [storage, hourly.times]);
  const peakSupply = Math.max(...hourly.totals, 0);
  const solarWind = dispatch.filter((item) => /solar|wind/i.test(item.carrier)).reduce((sum, item) => sum + Math.max(item.dispatch_mw, 0), 0);
  const imports = dispatch.filter((item) => /import/i.test(item.carrier)).reduce((sum, item) => sum + Math.max(item.dispatch_mw, 0), 0);
  const storageDischarge = storage.reduce((sum, item) => sum + Math.max(item.net_dispatch_mw, 0), 0);

  if (!completed.length) return <><PageIntro eyebrow="03 / HOUR BY HOUR" title="How does the plan work during a day?" text="See when each source supplies electricity and when storage fills or empties. These are simulated results, not records of what really happened." /><EmptyState title={online ? "No hourly results yet" : "Hourly results aren't available right now"} text={online ? "Run a scenario first, then come back to explore a day." : "Reconnect to Atlas to load scenarios and their hourly results."} /></>;

  return <>
    <PageIntro eyebrow="03 / HOUR BY HOUR" title="How does the plan work during a day?" text="See when each source supplies electricity and when storage fills or empties. These are simulated results, not records of what really happened." />
    <section className="panel operations-toolbar">
      <label>Scenario<select value={runId} onChange={(event) => chooseRun(event.target.value)}>{completed.map((item) => <option key={item.optimizer_run_id!} value={item.optimizer_run_id!}>{item.request.name}</option>)}</select></label>
      <div className="date-stepper"><button onClick={() => setDate(shiftDate(date, -1))} disabled={date <= `${planningYear}-01-01`} aria-label="Previous day">←</button><label>Day<input type="date" min={`${planningYear}-01-01`} max={`${planningYear}-12-31`} value={date} onChange={(event) => setDate(event.target.value)} /></label><button onClick={() => setDate(shiftDate(date, 1))} disabled={date >= `${planningYear}-12-31`} aria-label="Next day">→</button></div>
      <span className="bounded-badge">Showing one day</span>
    </section>
    {error && <p className="inline-error" role="alert">Could not load this day. {error}</p>}
    {loading ? <section className="panel chart-loading"><span className="loader" /><p>Loading this day…</p></section> : dispatch.length === 0 ? <EmptyState title="Nothing to show for this day" text="Try another day in the selected year." /> : <>
      <section className="operations-kpis">
        <KpiCard label="Peak supply before storage" value={`${compact.format(peakSupply)} MW`} detail="From power plants and imports" tone="sand" />
        <KpiCard label="Solar and wind today" value={`${compact.format(solarWind)} MWh`} detail="Energy generated in this day" tone="green" />
        <KpiCard label="Imported electricity" value={`${compact.format(imports)} MWh`} detail="Energy brought in this day" tone="red" />
        <KpiCard label="Electricity from storage" value={`${compact.format(storageDischarge)} MWh`} detail="Energy released in this day" tone="blue" />
      </section>
      <section className="panel hourly-chart-panel"><PanelHead title="Where electricity comes from each hour" subtitle={`${friendlyDate(date)} · simulated output in MW`} tag="One-day view" /><DispatchChart times={hourly.times} series={hourly.series} totals={hourly.totals} /></section>
      <div className="two-column operations-lower">
        <section className="panel storage-chart-panel"><PanelHead title="Energy held in storage" subtitle="Amount stored at the end of each hour · MWh" tag={`${storageSeries.length} storage units`} /><StorageChart times={hourly.times} series={storageSeries} /></section>
        <section className="panel hourly-table-panel"><PanelHead title="A few hours at a glance" subtitle="Every six hours and the day's highest output" tag="UTC" /><HourlyCheckpointTable times={hourly.times} totals={hourly.totals} series={hourly.series} /></section>
      </div>
      <p className="operations-note"><b>How to read this:</b> electricity use is a generated profile matched to public annual figures, and renewable output is estimated from weather. These charts show what the model would do—not measured grid operations.</p>
    </>}
  </>;
}

function DispatchChart({ times, series, totals }: { times: string[]; series: HourlySeries[]; totals: number[] }) {
  const width = 960, height = 330, left = 58, right = 18, top = 20, bottom = 42;
  const plotWidth = width - left - right, plotHeight = height - top - bottom;
  const max = Math.max(...totals, 1) * 1.08;
  const x = (index: number) => left + (times.length <= 1 ? 0 : index / (times.length - 1) * plotWidth);
  const y = (value: number) => top + plotHeight - value / max * plotHeight;
  let baseline = times.map(() => 0);
  const areas = series.map((item) => {
    const upper = item.values.map((value, index) => baseline[index] + Math.max(value, 0));
    const points = upper.map((value, index) => `${x(index)},${y(value)}`).concat([...baseline].reverse().map((value, reversed) => `${x(times.length - 1 - reversed)},${y(value)}`)).join(" ");
    baseline = upper;
    return { ...item, points };
  });
  return <div className="chart-wrap"><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Stacked modeled hourly generation and electricity imports by carrier">
    {[0, .25, .5, .75, 1].map((fraction) => <g key={fraction}><line x1={left} y1={y(max * fraction)} x2={width - right} y2={y(max * fraction)} className="chart-grid" /><text x={left - 10} y={y(max * fraction) + 4} textAnchor="end" className="axis-label">{compact.format(max * fraction)}</text></g>)}
    {areas.map((area) => <polygon key={area.name} points={area.points} fill={area.color} className="dispatch-area" />)}
    {times.map((time, index) => index % 3 === 0 && <text key={time} x={x(index)} y={height - 15} textAnchor="middle" className="axis-label">{hourLabel(time)}</text>)}
  </svg><ChartLegend series={series} /></div>;
}

function StorageChart({ times, series }: { times: string[]; series: HourlySeries[] }) {
  const width = 640, height = 290, left = 56, right = 18, top = 20, bottom = 40;
  const plotWidth = width - left - right, plotHeight = height - top - bottom;
  const max = Math.max(...series.flatMap((item) => item.values), 1) * 1.08;
  const x = (index: number) => left + (times.length <= 1 ? 0 : index / (times.length - 1) * plotWidth);
  const y = (value: number) => top + plotHeight - value / max * plotHeight;
  return <div className="chart-wrap"><svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Modeled hourly storage state of charge">
    {[0, .25, .5, .75, 1].map((fraction) => <g key={fraction}><line x1={left} y1={y(max * fraction)} x2={width - right} y2={y(max * fraction)} className="chart-grid" /><text x={left - 9} y={y(max * fraction) + 4} textAnchor="end" className="axis-label">{compact.format(max * fraction)}</text></g>)}
    {series.map((item) => <polyline key={item.name} points={item.values.map((value, index) => `${x(index)},${y(value)}`).join(" ")} fill="none" stroke={item.color} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" />)}
    {times.map((time, index) => index % 6 === 0 && <text key={time} x={x(index)} y={height - 14} textAnchor="middle" className="axis-label">{hourLabel(time)}</text>)}
  </svg><ChartLegend series={series} /></div>;
}

function ChartLegend({ series }: { series: HourlySeries[] }) { return <div className="chart-legend">{series.map((item) => <span key={item.name}><i style={{ background: item.color }} />{friendly(item.name)}</span>)}</div>; }

function HourlyCheckpointTable({ times, totals, series }: { times: string[]; totals: number[]; series: HourlySeries[] }) {
  const peakIndex = totals.indexOf(Math.max(...totals));
  const indexes = [...new Set([0, 6, 12, 18, peakIndex])].filter((index) => index >= 0 && index < times.length).sort((a, b) => a - b);
  const renewables = series.filter((item) => /solar|wind/i.test(item.name));
  return <div className="checkpoint-table"><div className="checkpoint-row head"><span>Hour</span><span>Supply</span><span>Solar + wind</span></div>{indexes.map((index) => <div className="checkpoint-row" key={times[index]}><span>{hourLabel(times[index])}{index === peakIndex && <small> peak</small>}</span><b>{compact.format(totals[index])} MW</b><span>{compact.format(renewables.reduce((sum, item) => sum + (item.values[index] ?? 0), 0))} MW</span></div>)}</div>;
}

function aggregateDispatch(items: DispatchPoint[]) {
  const times = [...new Set(items.map((item) => item.timestamp_utc))].sort();
  const carriers = [...new Set(items.map((item) => item.carrier))].sort((a, b) => carrierRank(a) - carrierRank(b));
  const series = carriers.map((carrier) => ({ name: carrier, color: carrierColor(carrier), values: times.map((time) => items.filter((item) => item.timestamp_utc === time && item.carrier === carrier).reduce((sum, item) => sum + Math.max(item.dispatch_mw, 0), 0)) }));
  return { times, series, totals: times.map((_, index) => series.reduce((sum, item) => sum + item.values[index], 0)) };
}

function aggregateStorage(items: StoragePoint[], times: string[]): HourlySeries[] {
  return [...new Set(items.map((item) => item.asset))].map((asset, index) => ({ name: asset, color: ["#84a8e2", "#63b9c6", "#b9e65a"][index % 3], values: times.map((time) => items.find((item) => item.timestamp_utc === time && item.asset === asset)?.state_of_charge_mwh ?? 0) }));
}

function carrierRank(carrier: string) { const order = ["hydro", "wind", "solar", "gas", "coal", "oil", "import", "bio", "load"]; const index = order.findIndex((name) => carrier.toLowerCase().includes(name)); return index < 0 ? order.length : index; }
function shiftDate(date: string, days: number) { const value = new Date(`${date}T00:00:00Z`); value.setUTCDate(value.getUTCDate() + days); return value.toISOString().slice(0, 10); }
function hourLabel(timestamp: string) { return new Date(timestamp).toISOString().slice(11, 16); }
function friendlyDate(date: string) { return new Intl.DateTimeFormat("en", { dateStyle: "long", timeZone: "UTC" }).format(new Date(`${date}T00:00:00Z`)); }

function ComparisonView({ online, completed, comparisons }: { online: boolean; completed: ScenarioRecord[]; comparisons: ScenarioComparison[] }) {
  const runs = useMemo(() => completed.map((item) => ({ id: item.optimizer_run_id!, name: item.request.name })), [completed]);
  const [base, setBase] = useState(runs[0]?.id ?? ""); const [candidate, setCandidate] = useState(runs[1]?.id ?? runs[0]?.id ?? "");
  useEffect(() => { if (!base && runs[0]) setBase(runs[0].id); if (!candidate && runs[1]) setCandidate(runs[1].id); }, [base, candidate, runs]);
  const direct = comparisons.find((item) => item.base_run_id === base && item.comparison_run_id === candidate);
  const reverse = comparisons.find((item) => item.base_run_id === candidate && item.comparison_run_id === base);
  const factor = direct ? 1 : -1; const comparison = direct ?? reverse;
  if (!online && !runs.length) return <><PageIntro eyebrow="04 / COMPARE PLANS" title="What changes between two plans?" text="Choose two scenarios to see the difference in cost, electricity sources, emissions and unmet demand." /><EmptyState title="Comparisons aren't available right now" text="Reconnect to Atlas to load scenarios and their differences." /></>;
  return <><PageIntro eyebrow="04 / COMPARE PLANS" title="What changes between two plans?" text="Choose two scenarios to see the difference in cost, electricity sources, emissions and unmet demand." />
    <section className="panel compare-picker"><label>First plan<select value={base} onChange={(event) => setBase(event.target.value)}>{runs.map((run) => <option key={run.id} value={run.id}>{run.name}</option>)}</select></label><span>→</span><label>Compare with<select value={candidate} onChange={(event) => setCandidate(event.target.value)}>{runs.map((run) => <option key={run.id} value={run.id}>{run.name}</option>)}</select></label></section>
    {comparison && base !== candidate ? <section className="comparison-grid" aria-label="Difference from the first plan"><Delta label="Estimated yearly cost" value={factor * comparison.annual_cost_delta_eur} format={money} /><Delta label="Cost per unit of electricity" value={factor * comparison.average_cost_delta_eur_mwh} format={moneyPerMwh} /><Delta label="Electricity from renewables" value={factor * comparison.renewable_share_delta} format={pct} /><Delta label="Estimated CO₂ emissions" value={factor * comparison.emissions_delta_tco2} format={(value) => `${compact.format(value)} tCO₂`} /><Delta label="Imported electricity" value={factor * comparison.imports_delta_mwh} format={(value) => `${compact.format(value)} MWh`} /><Delta label="Electricity demand not met" value={factor * comparison.unserved_delta_mwh} format={(value) => `${decimal.format(value)} MWh`} /></section> : <EmptyState title={runs.length < 2 ? "Run another scenario to compare" : "Choose two different scenarios"} text={runs.length < 2 ? "Once two scenarios are ready, you can see their differences here." : "The two dropdowns must show different plans."} />}
    <p className="comparison-caveat">A lower modeled cost does not automatically make a plan better in the real world. Check the assumptions and limits before drawing conclusions.</p></>;
}
function Delta({ label, value, format }: { label: string; value: number; format: (value: number) => string }) {
  const formatted = `${value > 0 ? "+" : ""}${format(value)}`;
  return <article className="delta-card"><span>{label}</span><b>{formatted}</b><small>{value === 0 ? "No change" : value > 0 ? "Higher than the first plan" : "Lower than the first plan"}</small></article>;
}

function TransparencyView({ kpi, provenance, presets }: { kpi: ScenarioKpi | null; provenance: ProvenanceRecord[]; presets: ScenarioPreset[] }) {
  const classes = [
    ["Reported", "OBSERVED", "A figure printed by a public source."],
    ["Estimated weather", "REANALYSIS", "Weather reconstructed from observations and computer models."],
    ["Calculated", "DERIVED", "A value worked out from source data using a documented rule."],
    ["Chosen input", "ASSUMPTION", "A price, target or other choice supplied to the model."],
    ["Generated from real totals", "SYNTHETIC_CALIBRATED", "An hourly pattern shaped to match reported annual figures—not measured hourly use."],
    ["Generated for the model", "SYNTHETIC", "A simulated profile, such as renewable output; not measured plant production."],
  ];
  return <><PageIntro eyebrow="05 / DATA & LIMITS" title="Where do these numbers come from?" text="Some figures come from reports. Others are calculated, estimated from weather or chosen for a scenario. The labels below tell you which is which." />
    <section className="provenance-classes" aria-label="Types of data used by Atlas">{classes.map(([label, code, description]) => <article key={code}><h2>{label}</h2><span>{code}</span><p>{description}</p></article>)}</section>
    <p className="operations-note"><b>Two units worth knowing:</b> MW measures how much power can be supplied at one moment. MWh measures electricity produced or used over time. None of the hourly results here are measured utility operations.</p>
    <div className="two-column transparency-columns"><section className="panel"><PanelHead title="About the selected result" subtitle={kpi?.scenario_name ?? "No scenario selected"} tag="DATA DETAILS" />{kpi ? <dl className="detail-list"><div><dt>Result ID</dt><dd>{kpi.run_id}</dd></div><div><dt>Model version</dt><dd>{kpi.model_version}</dd></div><div><dt>Input data version</dt><dd>{kpi.input_package_version ?? "Not recorded"}</dd></div><div><dt>Weather used</dt><dd>{kpi.weather_year}</dd></div>{provenance.slice(0, 8).map((item) => <div key={item.provenance_key}><dt>{friendly(item.provenance_key)}</dt><dd><ProvenanceValue value={item.provenance_value} /></dd></div>)}</dl> : <p className="muted">Choose a scenario on Results to see its data details.</p>}</section><section className="panel"><PanelHead title="Saved example scenarios" subtitle="Starting choices you can adjust" tag={`${presets.length} examples`} /><div className="catalog-list">{presets.map((preset) => <article key={preset.preset_id}><div><b>{preset.request.name}</b><span>v{preset.provenance.version} · {preset.provenance.classification}</span></div><p>{preset.provenance.note}</p></article>)}</div></section></div></>;
}

function ProvenanceValue({ value }: { value: unknown }) {
  if (value === null || typeof value !== "object") return <>{String(value ?? "Not recorded")}</>;
  const count = Array.isArray(value) ? value.length : Object.keys(value).length;
  return <details className="provenance-detail"><summary>Show {count} source {Array.isArray(value) ? "entries" : "fields"}</summary><pre>{JSON.stringify(value, null, 2)}</pre></details>;
}

function aggregateCosts(costs: CostResult[]) { const groups = new Map<string, number>(); costs.forEach((row) => groups.set(row.component_type, (groups.get(row.component_type) ?? 0) + row.amount_eur)); return [...groups.entries()].map(([label, value]) => ({ label, value })).filter((item) => Math.abs(item.value) > 0.01).sort((a, b) => b.value - a.value); }
function donutGradient(items: { value: number }[], total: number) { let start = 0; const stops = items.map((item, index) => { const end = start + item.value / total * 100; const stop = `${COST_COLORS[index % COST_COLORS.length]} ${start}% ${end}%`; start = end; return stop; }); return `conic-gradient(${stops.join(",")})`; }
function carrierColor(carrier: string) { const match = Object.keys(CARRIER_COLORS).find((key) => carrier.toLowerCase().includes(key)); return match ? CARRIER_COLORS[match] : "#87928d"; }
function friendly(value: string) { return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()); }
function humanError(value: string) { try { const parsed = JSON.parse(value); return parsed.detail?.[0]?.msg ?? parsed.detail ?? value; } catch { return value; } }
