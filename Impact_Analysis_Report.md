1. Addressed Component
This impact analysis follows the impact analysis process described in Chapter 6, including identification of the Starting Impact Set (SIS), construction of a dependency-based impact analysis model, and reasoning about the Candidate Impact Set (CIS) and potential ripple effects.
REST Controller Layer

Security and Access Control Layer

Core Service Layer

Rule Engine Subsystem

Persistence Layer (DAO)

Database

The analysis targets how incoming API requests and device-related operations propagate through these backend layers and how changes in one component may impact others.
2. Impact Analysis Graph and Its Completeness
For this assignment, a Program Dependency Graph was created to represent the architectural-level dependencies within the ThingsBoard backend system.The SIS for this analysis consists of representative REST controllers (e.g., DeviceController) and their exposed API endpoints, which serve as the initial entry points for change requests affecting device-related functionality.

<img width="437" height="621" alt="截屏2026-01-04 16 05 23" src="https://github.com/user-attachments/assets/0c422fdc-1ccb-4713-a70c-de0f3d6a95fd" />



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


4.From an adequacy perspective, the analysis aims to include all components along the backend request-processing pipeline, reducing the risk of missing impacted elements.
In terms of effectiveness, the abstraction at the layer level helps limit false positives by avoiding overly fine-grained method-level dependencies, making the analysis practical for a large legacy system.
