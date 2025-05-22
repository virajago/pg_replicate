# API Endpoints

This document lists the exposed API endpoints for the service.

**Health Check**
*   `GET /v1/health_check`: Checks the health status of the API.

**Images**
*   `POST /v1/images`: Creates a new image.
*   `GET /v1/images`: Lists all images.
*   `GET /v1/images/{image_id}`: Retrieves a specific image by its ID.
*   `PUT /v1/images/{image_id}`: Updates a specific image.
*   `DELETE /v1/images/{image_id}`: Deletes a specific image.

**Pipelines**
*   `POST /v1/tenants/{tenant_id}/pipelines`: Creates a new pipeline for a tenant.
*   `GET /v1/tenants/{tenant_id}/pipelines`: Lists all pipelines for a tenant.
*   `GET /v1/tenants/{tenant_id}/pipelines/{pipeline_id}`: Retrieves a specific pipeline for a tenant.
*   `PUT /v1/tenants/{tenant_id}/pipelines/{pipeline_id}`: Updates a specific pipeline for a tenant.
*   `DELETE /v1/tenants/{tenant_id}/pipelines/{pipeline_id}`: Deletes a specific pipeline for a tenant.
*   `POST /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/start`: Starts a specific pipeline.
*   `POST /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/stop`: Stops a specific pipeline.
*   `POST /v1/tenants/{tenant_id}/pipelines/{pipeline_id}/restart`: Restarts a specific pipeline.

**Sinks**
*   `POST /v1/tenants/{tenant_id}/sinks`: Creates a new sink for a tenant.
*   `GET /v1/tenants/{tenant_id}/sinks`: Lists all sinks for a tenant.
*   `GET /v1/tenants/{tenant_id}/sinks/{sink_id}`: Retrieves a specific sink for a tenant.
*   `PUT /v1/tenants/{tenant_id}/sinks/{sink_id}`: Updates a specific sink for a tenant.
*   `DELETE /v1/tenants/{tenant_id}/sinks/{sink_id}`: Deletes a specific sink for a tenant.

**Sinks-Pipelines Associations**
*   `POST /v1/tenants/{tenant_id}/sinks_pipelines`: Associates a sink with a pipeline for a tenant.
*   `GET /v1/tenants/{tenant_id}/sinks_pipelines`: Lists all sink-pipeline associations for a tenant.
*   `DELETE /v1/tenants/{tenant_id}/sinks_pipelines/pipelines/{pipeline_id}/sinks/{sink_id}`: Deletes a specific sink-pipeline association for a tenant.

**Sources**
*   `POST /v1/tenants/{tenant_id}/sources`: Creates a new source for a tenant.
*   `GET /v1/tenants/{tenant_id}/sources`: Lists all sources for a tenant.
*   `GET /v1/tenants/{tenant_id}/sources/{source_id}`: Retrieves a specific source for a tenant.
*   `PUT /v1/tenants/{tenant_id}/sources/{source_id}`: Updates a specific source for a tenant.
*   `DELETE /v1/tenants/{tenant_id}/sources/{source_id}`: Deletes a specific source for a tenant.
*   `GET /v1/tenants/{tenant_id}/sources/{source_id}/publications`: Lists publications for a source.
*   `POST /v1/tenants/{tenant_id}/sources/{source_id}/publications`: Creates a publication for a source.
*   `DELETE /v1/tenants/{tenant_id}/sources/{source_id}/publications/{publication_name}`: Deletes a publication from a source.
*   `GET /v1/tenants/{tenant_id}/sources/{source_id}/tables`: Lists tables for a source.
*   `POST /v1/tenants/{tenant_id}/sources/{source_id}/tables`: Adds a table to a source's publication.
*   `DELETE /v1/tenants/{tenant_id}/sources/{source_id}/tables/{table_name}`: Removes a table from a source's publication.

**Tenants**
*   `POST /v1/tenants`: Creates a new tenant.
*   `GET /v1/tenants`: Lists all tenants.
*   `GET /v1/tenants/{tenant_id}`: Retrieves a specific tenant by its ID.
*   `PUT /v1/tenants/{tenant_id}`: Updates a specific tenant.
*   `DELETE /v1/tenants/{tenant_id}`: Deletes a specific tenant.

**Tenants-Sources Associations**
*   `POST /v1/tenants_sources`: Associates a tenant with a source.
*   `GET /v1/tenants_sources`: Lists all tenant-source associations.
*   `DELETE /v1/tenants_sources/tenants/{tenant_id}/sources/{source_id}`: Deletes a specific tenant-source association.
