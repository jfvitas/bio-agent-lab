# Model Studio Backlog

## Current Priority Order

1. Backend parity and reliability
2. Portable/runtime parity across local, cluster, Kaggle, and Colab
3. GUI polish and workflow clarity

## Backend Work Still Worth Doing

- Broaden native portable execution coverage beyond the current `gnn` baseline.
- Add stronger native hybrid regression coverage and import/export parity checks.
- Expand saved-model inference to support more imported remote-run layouts without manual path cleanup.
- Add batch inference and richer output manifests for downstream analysis.
- Add more unsupervised backends beyond the current PCA/KMeans exploration baseline.

## GUI / UX Work Explicitly Queued

- Reduce density in the right-side overview and Model Studio panes.
- Add richer in-GUI chart and artifact previews instead of mostly text/json summaries.
- Improve visual hierarchy, spacing, and typography for a more modern feel.
- Add clearer step-by-step workflow guidance for first-time users.
- Revisit overall layout polish after the backend surface stabilizes.
