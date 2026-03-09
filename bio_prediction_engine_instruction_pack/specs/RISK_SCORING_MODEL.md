
# Risk Scoring Model

Risk score integrates:

- binding strength
- pathway overlap
- target essentiality
- tissue expression

Example scoring formula:

risk_score =
(binding_weight * predicted_affinity)
+ (pathway_overlap_weight * pathway_similarity)
