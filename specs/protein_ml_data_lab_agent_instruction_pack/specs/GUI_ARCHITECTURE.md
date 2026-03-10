# GUI Architecture Specification

Framework: **PySide6 (Qt)**

The interface must be a **single window workflow-driven GUI**.

## Layout

+---------------------------------------------------+
| Menu Bar                                          |
+---------------------------------------------------+
| Sidebar Workflow Steps                            |
|                                                   |
| 1 Workspace                                       |
| 2 Protein Search                                  |
| 3 Metadata Harvest                                |
| 4 Structure Download                              |
| 5 Feature Extraction                              |
| 6 Graph Builder                                   |
| 7 Dataset Builder                                 |
|                                                   |
+----------------+----------------------------------+
| Workflow Panel | Output Console                   |
|                |                                  |
|                |                                  |
|                |                                  |
+----------------+----------------------------------+

## Scroll Behavior

All scrollable panels must allow **mouse wheel scrolling anywhere in the panel**, not just over scroll bars.

Implementation:

- install eventFilter
- capture wheel events
- route to parent scrollArea

## Threading

Long operations must run in **worker threads**:

Examples:

- database search
- structure downloads
- PyRosetta relaxation
- graph generation

Use Qt signal/slot system to report progress.

## Progress Indicators

Every long operation must show:

- progress bar
- console log output