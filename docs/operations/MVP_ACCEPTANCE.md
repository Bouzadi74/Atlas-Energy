# Academic MVP acceptance

Atlas is ready for an academic demonstration only when one newly submitted scenario completes the
actual asynchronous path and its published results reconcile through the same Next.js proxy used
by the browser. Unit tests, a successful standalone optimizer run, or an Airflow DAG import alone
do not establish that outcome.

## Repeat the acceptance check

1. Start PostgreSQL and Kafka, FastAPI, the outbox publisher, the scenario worker, and the Next.js
   frontend using the root `README.md`. Keep the heavy Airflow DAGs paused; they are not part of
   interactive scenario execution.
2. In the Scenario Builder at <http://localhost:3000>, submit a **distinct** scenario. Record the
   returned `job_id`. Reusing an unchanged request tests the cache, not a new optimization.
3. From the repository root, run:

   ```powershell
   uv run --no-sync python scripts/acceptance_check.py --job-id job_REPLACE_ME
   ```

   The checker is read-only. It waits for `completed`, verifies the scenario/run links, reads
   KPI, capacity, bounded dispatch and storage, cost, provenance, and comparison endpoints through
   the frontend proxy, and reconciles cost components to the annual system-cost KPI. It exits
   nonzero on a failed job, timeout, incomplete result, or inconsistent identifier.
4. Open the same completed scenario in the browser. Confirm the executive cards, capacity and
   cost views, comparison, hourly operations, and model transparency render without an error.

The script checks the API contract and proxy, but it cannot prove visual layout or interaction
quality. The browser check remains a human acceptance step.

## Repeat the local browser check

With the API and frontend running, start a temporary Chrome DevTools target in a separate
PowerShell window (Chrome must already be installed):

```powershell
& 'C:\Program Files\Google\Chrome\Application\chrome.exe' `
  --headless=old --disable-gpu --disable-software-rasterizer `
  --remote-debugging-address=127.0.0.1 --remote-debugging-port=9223 `
  --user-data-dir="$env:TEMP\atlas-browser-acceptance" about:blank
```

From the repository root, run `node scripts/browser_acceptance.mjs`. It opens the local frontend,
checks the five rendered views, waits for hourly data and comparison deltas, checks mobile-width
overflow on all five views, and saves ignored screenshots under `data/browser_acceptance/`.
Set `ATLAS_BROWSER_SUBMIT_REUSED=1` to also submit the existing Baseline preset through the
browser and confirm that its job is completed; the default check is read-only. Stop the temporary
Chrome process afterward. This automated check does not replace a keyboard/screen-reader review or
human inspection of the charts and longer pages.

## Status and limitations

On 3 October 2026, an API-submitted custom 2027 scenario completed via Kafka, PyPSA/HiGHS,
atomic PostgreSQL publication, and dbt. Job `job_35b9e8ba2e8c6d1b` links to scenario
`scn_35b9e8ba2e8c6d1b` and optimizer run `e90ef9487ac87bb4`. The acceptance checker passed
through the Next.js proxy: 15 capacity rows, bounded dispatch and storage pages, 30 cost rows,
four provenance records, and 15 current pairwise comparisons. Cost components reconciled to
€3,078,515,402.67 annual modeled system cost. This is a model output, not a budget or forecast.
PostgreSQL contains exactly one published run with that ID, 8,760 distinct dispatch hours, and
8,760 distinct storage hours. Re-submitting the same request returned `reused=true` and the
original job ID; the transactional outbox had no unpublished events afterward.

The code suite passed 86 tests with one opt-in Spark integration test skipped; dbt passed all 71
operations with no warnings; the Next.js production build and strict TypeScript check passed.
An earlier intentionally retained failed job exposed and helped fix dbt executable discovery.
Its published optimizer artifact is preserved, while its failed run is excluded from current KPI
serving and pairwise comparisons. The browser-control helper did not initialize, so the initial
acceptance run did not include visual inspection. A subsequent headless Chrome walkthrough on
3 October 2026 rendered all five desktop and mobile views, verified populated hourly and
comparison views, found no mobile horizontal overflow, and submitted an existing Baseline preset
through the browser to its completed job. It exposed a misleading capacity chart that included
reliability slack and import limits; the chart now shows domestic generation/storage capacity only.
Long provenance JSON is now available in expandable records. Strict TypeScript and the production
Next.js build passed after those corrections. A human keyboard/screen-reader walkthrough remains.

The source data, 2024 calibration, published scenario catalog, and model outputs are academic
decision-support artifacts. Hourly demand is synthetic-calibrated, weather is reanalysis, and
renewable output profiles are synthetic; none is measured ONEE dispatch. The local demo does not
have end-user identity, TLS, a secret manager, alert routing, backups, a real utility telemetry
source, or a production Airflow deployment. It must not be presented as a production service or
investment recommendation.

If a job exhausts its retries, preserve its failed record for diagnosis. Correct the cause and
submit a distinct scenario through the application rather than editing the database or deleting
optimizer artifacts to make the demo appear green.
