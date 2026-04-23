# Procurement Analysis: Meridian Industrial

These analyses answer six operational questions a procurement 
manager or VP would actually ask about their spend data.

Each query is written as a standalone SQL file against the 
Meridian Industrial Delta tables and is designed to be 
readable without technical context.

## Questions Answered

1. Where is spend concentrated, and is that concentration a risk?
2. Are higher-tier suppliers actually outperforming on delivery?
3. Where are we paying above-contract pricing?
4. Which invoices show signs of overbilling?
5. Which supplier relationships carry the most combined risk?
6. How complete is our three-way match coverage?

## Dashboard

These queries power the Meridian Procurement Analytics Lakeview 
dashboard, which surfaces four executive KPIs (Total Active 
Spend, Suppliers with HIGH Risk, Overbilling Exposure, and 
Invoices Failing Three-Way Match) alongside three operational 
charts. Queries 5 and 6 each include a dashboard KPI variant 
at the end of the file.

## Analytical Decisions

**Q5 (Supplier Risk Flags):** Delivery risk thresholds are 
calibrated to the actual OTIF distribution in the data 
(HIGH < 65%, MEDIUM < 80%). Billing risk thresholds reflect 
a dataset-wide overbilling rate of 6.2% (HIGH > 20%, 
MEDIUM > 10%).

**Q6 (Three-Way Match):** A 5% tolerance is applied to the 
amount variance check to account for legitimate invoice 
variances including freight, taxes, and rounding. A fixed 
dollar threshold is inappropriate at this spend scale.
