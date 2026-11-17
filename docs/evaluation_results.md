# Agent Evaluation Results

## Objective

The evaluation assesses whether the marketing analytics agent:

1. selects the appropriate Unity Catalog function;
2. extracts the correct function parameters;
3. bases quantitative conclusions on tool results;
4. avoids inventing unavailable metrics.

## Test cases

| Test question | Expected tool | Expected parameter | Result |
|---|---|---|---|
| Which five campaigns require the most attention? | get_campaigns_requiring_attention | max_campaigns = 5 | Pass |
| Which marketing channel performs best? | get_channel_performance | None | Pass |
| Which marketing channel performs best in Germany? | get_country_performance | requested_country = Germany | Pass |
| Which three campaigns should be investigated first and why? | get_campaigns_requiring_attention | max_campaigns = 3 | Pass |
| What was the customer satisfaction score for Campaign C0001? | No supported tool | None | Pass (agent states metric is unavailable) |

## Notes

The evaluation uses synthetic campaign data.

The proof of concept demonstrates functional tool selection, parameter
extraction and grounding in analytical tool results. It is not intended
to constitute a comprehensive production-grade agent evaluation.
