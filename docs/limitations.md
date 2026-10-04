# Limitations

- Public Moroccan data may not include utility-grade hourly national demand, dispatch, outage, or network constraints.
- Any generated hourly curve must be labeled `SYNTHETIC_CALIBRATED`; matching annual energy and peak does not reconstruct historical operations.
- The MVP is a single national node and cannot reveal regional congestion or transmission investment needs.
- A linear, perfect-foresight optimization omits unit commitment, detailed reserves, forecast uncertainty, market behavior, and many operational constraints.
- Scenario outputs are model results, not investment advice. Recommendations require human review and sensitivity analysis.
- The baseline aggregates existing CSP with the solar-PV availability profile, represents reservoir
  hydro with a flat inflow constrained by an annual energy budget, and assumes fixed storage
  durations and cyclic state of charge.
- Imports use an assumed fixed capacity, price, and emissions factor; the model does not represent
  interconnector counterpart markets or endogenous export decisions.
