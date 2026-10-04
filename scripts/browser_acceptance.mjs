// Run against a local Chrome instance started with --remote-debugging-port=9223.
// This checks rendered views without changing scenario or database state.
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const endpoint = process.env.ATLAS_CDP_URL ?? "http://127.0.0.1:9223";
const appUrl = process.env.ATLAS_FRONTEND_URL ?? "http://localhost:3000";
const outputDir = resolve(process.env.ATLAS_BROWSER_OUTPUT_DIR ?? "data/browser_acceptance");
const submitReused = process.env.ATLAS_BROWSER_SUBMIT_REUSED === "1";
const delay = (ms) => new Promise((done) => setTimeout(done, ms));

async function target() {
  const response = await fetch(`${endpoint}/json/new?${encodeURIComponent(appUrl)}`, { method: "PUT" });
  if (!response.ok) throw new Error(`Cannot open browser target: ${response.status}`);
  return response.json();
}

function connect(url) {
  const socket = new WebSocket(url);
  const pending = new Map();
  let nextId = 0;
  socket.addEventListener("message", ({ data }) => {
    const message = JSON.parse(data);
    if (!message.id) return;
    const wait = pending.get(message.id);
    if (!wait) return;
    pending.delete(message.id);
    if (message.error) wait.reject(new Error(JSON.stringify(message.error)));
    else wait.resolve(message.result);
  });
  const ready = new Promise((resolveReady, rejectReady) => {
    socket.addEventListener("open", resolveReady, { once: true });
    socket.addEventListener("error", rejectReady, { once: true });
  });
  return {
    ready,
    close: () => socket.close(),
    command: (method, params = {}) => new Promise((resolveResult, rejectResult) => {
      const id = ++nextId;
      pending.set(id, { resolve: resolveResult, reject: rejectResult });
      socket.send(JSON.stringify({ id, method, params }));
    }),
  };
}

async function main() {
  await mkdir(outputDir, { recursive: true });
  const page = await target();
  const client = connect(page.webSocketDebuggerUrl);
  await client.ready;
  const command = client.command;
  const evaluate = async (expression) => {
    const result = await command("Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.text);
    return result.result.value;
  };
  const until = async (expression, label) => {
    for (let attempt = 0; attempt < 150; attempt += 1) {
      if (await evaluate(expression)) return;
      await delay(200);
    }
    const visibleText = await evaluate("document.body?.innerText.slice(0, 1200)");
    throw new Error(`Timed out waiting for ${label}: ${visibleText}`);
  };
  const screenshot = async (name) => {
    const result = await command("Page.captureScreenshot", { format: "png", captureBeyondViewport: false });
    const file = resolve(outputDir, name);
    await writeFile(file, Buffer.from(result.data, "base64"));
    console.log(`Screenshot: ${file}`);
  };
  const clickNav = async (label) => {
    const clicked = await evaluate(`(() => {
      const button = [...document.querySelectorAll('nav button')].find((node) => node.getAttribute('aria-label') === ${JSON.stringify(label)});
      if (!button) return false;
      button.click();
      return true;
    })()`);
    if (!clicked) throw new Error(`Navigation button missing: ${label}`);
  };

  try {
    await command("Page.enable");
    await command("Runtime.enable");
    await command("Emulation.setDeviceMetricsOverride", { width: 1440, height: 900, deviceScaleFactor: 1, mobile: false });
    await until("document.body?.innerText.includes('Estimated yearly cost')", "scenario results");
    const overview = await evaluate("document.body.innerText");
    if (!overview.includes("ready to explore") || overview.includes("Atlas couldn't load the data")) {
      throw new Error("Results page is missing scenarios or shows an API error");
    }
    await screenshot("overview-desktop.png");
    console.log("PASS results: scenario estimates rendered");

    for (const [label, marker, name] of [
      ["Try a scenario", "Adjust your assumptions", "builder-desktop.png"],
      ["Hour by hour", "How does the plan work during a day?", "operations-desktop.png"],
      ["Compare plans", "What changes between two plans?", "compare-desktop.png"],
      ["Data & limits", "About the selected result", "transparency-desktop.png"],
    ]) {
      await clickNav(label);
      await until(`document.querySelector('main')?.innerText.includes(${JSON.stringify(marker)})`, label);
      await delay(400);
      const body = await evaluate("document.querySelector('main').innerText");
      if (body.includes("Atlas couldn't load the data")) throw new Error(`${label} shows an API error`);
      if (label === "Try a scenario") {
        const presetCount = await evaluate("document.querySelectorAll('.preset-grid button').length");
        if (presetCount < 6) throw new Error("Scenario builder is missing presets or custom case");
        if (submitReused) {
          const clicked = await evaluate(`(() => {
            const button = [...document.querySelectorAll('.submit-row button')].find((node) => node.textContent.includes('Run this scenario'));
            if (!button) return false;
            button.click();
            return true;
          })()`);
          if (!clicked) throw new Error("Scenario submission button missing");
          await until("document.querySelector('.job-title b')?.textContent === 'Ready to view'", "reused scenario completion");
          console.log("PASS scenario builder: existing preset returned completed job");
        }
      }
      if (label === "Hour by hour") {
        await until("document.querySelector('.hourly-chart-panel svg') && !document.querySelector('.chart-loading')", "hourly chart");
      }
      if (label === "Compare plans") {
        const deltas = await evaluate("document.querySelectorAll('.delta-card').length");
        if (deltas !== 6) throw new Error(`Expected six comparison deltas; got ${deltas}`);
      }
      if (label === "Data & limits") {
        const records = await evaluate("document.querySelectorAll('.provenance-detail').length");
        if (records < 1) throw new Error("Provenance disclosures are missing");
      }
      await screenshot(name);
      console.log(`PASS ${label}`);
    }

    await command("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });
    for (const [label, marker, name] of [
      ["Results", "Estimated yearly cost", "overview-mobile.png"],
      ["Try a scenario", "Adjust your assumptions", "builder-mobile.png"],
      ["Hour by hour", "How does the plan work during a day?", "operations-mobile.png"],
      ["Compare plans", "What changes between two plans?", "compare-mobile.png"],
      ["Data & limits", "About the selected result", "transparency-mobile.png"],
    ]) {
      await clickNav(label);
      await until(`document.querySelector('main')?.innerText.includes(${JSON.stringify(marker)})`, `mobile ${label}`);
      await delay(300);
      const horizontalOverflow = await evaluate("document.documentElement.scrollWidth > window.innerWidth + 1");
      if (horizontalOverflow) throw new Error(`Mobile ${label} has horizontal overflow`);
      await screenshot(name);
      console.log(`PASS mobile ${label}: no horizontal overflow`);
    }
  } finally {
    client.close();
    await fetch(`${endpoint}/json/close/${page.id}`).catch(() => {});
  }
}

await main();
