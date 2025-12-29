1. Addressed Component
This impact analysis focuses on the backend architectural components of the ThingsBoard legacy system, specifically the dependency relationships between the following layers:

REST Controller Layer

Security and Access Control Layer

Core Service Layer

Rule Engine Subsystem

Persistence Layer (DAO)

Database

The analysis targets how incoming API requests and device-related operations propagate through these backend layers and how changes in one component may impact others.
2. Impact Analysis Graph and Its Completeness
For this assignment, a Program Dependency Graph was created to represent the architectural-level dependencies within the ThingsBoard backend system.

<img width="413" height="628" alt="截屏2025-12-29 17 27 02" src="https://github.com/user-attachments/assets/dce123c4-23ba-49bd-8620-bada0f142f1b" />


Graph Description

The Program Dependency Graph illustrates:

How the REST Controller Layer depends on the Security and Access Control Layer for authorization.

How authorized requests are forwarded to the Core Service Layer.

How the Core Service Layer interacts with the Rule Engine Subsystem and the Persistence Layer (DAO).

How the DAO layer acts as the only access point to the Database.

Completeness Justification

Verification: Representative controllers (e.g., DeviceController) and Service components were inspected to confirm dependencies.

Coverage: The graph covers the full backend request-processing pipeline, from entry to persistence.

Traceability: All nodes and directions are clearly labeled to show change propagation.

3. Impact or Insights Gained from the Analysis

Insight 1: Security Layer as a Cross-Cutting Dependency

Impact: Any change in access control logic can affect all REST API endpoints.

Insight: Highlights the need to isolate security concerns during reengineering.

Insight 2: Core Service Layer as a Dependency Hub

Impact: Modifications to core services may propagate to both rule processing and data persistence.

Insight: This is a high-risk area requiring careful regression testing.

Insight 3: Tight Coupling Between Services and Persistence

Impact: Changes in database schemas can affect multiple services through the DAO layer.

Insight: Suggests an opportunity for further abstraction to improve maintainability.
