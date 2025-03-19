# Changelog
## 0.22.4 (2025-03-19)
- Change one `FILTER` from `LoketLB-toezichtGebruiker` to `LoketLB-berichtenGebruiker`.
  - The query looks for `schema:Conversation`, so we were filtering on the wrong graph.
## 0.22.3 (2025-03-18)
- Limit `SELECT` queries to organization graphs only. [DL-6493]
