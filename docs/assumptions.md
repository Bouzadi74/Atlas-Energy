# Assumptions register

Every assumption must record a name, value, unit, applicable year/region/technology, source or rationale, retrieval/publication date where relevant, and version.

All files under `configs/scenarios/` are versioned academic sensitivities classified as
`ASSUMPTION`. They are suitable for comparing model behavior, not for making investment claims or
representing an official Moroccan forecast.

The current catalog contains:

- `baseline-2030`: the reference assumptions used for comparison;
- `high-renewables-2035`: a combined policy-transition sensitivity;
- `high-fuel-carbon-2030`: isolates higher gas and carbon prices;
- `accelerated-demand-2035`: isolates higher annual demand growth;
- `low-clean-tech-cost-2035`: tests lower solar, wind, and battery capital costs.

Each YAML file carries a provenance classification, scenario version, and explanatory note. The
API validates the complete on-disk contract before exposing a preset.

Required scenario assumption families:

- annual demand and growth;
- fuel and carbon prices;
- technology CAPEX, fixed/variable OPEX, lifetime, efficiency, and availability;
- renewable resource year and capacity-factor method;
- storage efficiencies, duration bounds, and cyclic state-of-charge rule;
- import/export limits and prices;
- renewable policy target and reliability penalty.
