# Baseline 2030 plausibility review

This review interprets corrected run `7ce9e36bc50405b2`. It is an academic model result based on
assumptions and synthetic/calibrated profiles, not an operational forecast or investment plan.

## Why the model builds 17.044 GW of solar

New solar has a configured annualized cost of about EUR 48,135/MW-year, zero variable cost, and a
national synthetic availability profile. This is substantially cheaper per MW-year than new wind
in the current assumption set. The model therefore combines solar with 5.070 GW / 20.281 GWh of
four-hour batteries to move daytime production into other hours. It also builds 4.786 GW of wind
because wind's profile diversifies solar and reduces the storage and firm-supply requirement.

The 50% minimum renewable-generation constraint is not binding in the final annual result: the
reported share is 72.52%. The build is therefore driven primarily by the modeled cost/profile
trade-off, hourly reliability, the 2.5 GW import limit, existing fleet, and storage physics—not by
forcing the solution to stop at the policy floor.

## Imports and firm supply

Imports provide 13.458 TWh, about 24.7% of annual demand, at the assumed EUR 90/MWh and fixed
2.5 GW interconnector limit. Coal provides 2.406 TWh. Existing and new gas and oil produce zero in
this solution because their configured fuel, carbon, and variable costs are not competitive at the
margin. This heavy import reliance is a key sensitivity to test, not a recommendation.

Reservoir hydro now supplies exactly 321 GWh and cannot charge from the grid. Pumped hydro and the
battery may charge and discharge; their discharge is not counted as primary renewable generation.

## What EUR 3.416 billion includes

- EUR 1.923 billion of annualized capital plus fixed O&M for new solar, wind, and battery capacity;
- EUR 1.493 billion of dispatch-related operating cost, including modeled fuel, variable O&M,
  carbon for domestic thermal generators, import purchases, and any reliability penalty;
- zero reliability penalty in this run because unmet demand is zero.

It does not include sunk existing-asset capital, comprehensive existing fixed O&M, transmission or
distribution expansion, reserves, startup/ramping costs, taxes, financing structure beyond the
configured capital-recovery method, or an endogenous neighboring electricity market. Imported
electricity uses a fixed all-in marginal price and reported emissions factor; the scenario carbon
price is not separately added to imports.

## Decision-use caveats and priority sensitivities

Before business interpretation, run sensitivities for import capacity/price, solar and battery
CAPEX, gas and carbon prices, renewable target, demand growth, weather year, hydro availability,
and storage duration. The national copper-plate result cannot determine regional siting or grid
reinforcement needs.
