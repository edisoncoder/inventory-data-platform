# Architecture

The system follows a layered data architecture:

* Raw: stores data as received
* Staging: applies transformations and cleaning
* Analytics: provides business-ready data models

The pipeline ensures separation of concerns and traceability across all stages.

Execution is orchestrated via Prefect, allowing monitoring, retries, and scheduling.
