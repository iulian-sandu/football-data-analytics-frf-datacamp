# Football Data Analytics - Superliga Datacamp - FRF

## Overview

End-to-end football data analytics flow built on Google Cloud services. The current implementation uses dummy data and some manually created resources. In the future implementation, infrastructure provisioning will be fully automated and managed through a GitOps process.

## Features and functionality

1. Trigger a Python Cloud Run service using Cloud Scheduler.
2. Simulate API scraping and store the data in a Cloud Storage bucket as a JSONL file for data backup purposes.
3. Upload data to BigQuery and apply data transformations. Save the transformed data in a new table.
4. Use Looker Studio to create a dashboard based on the transformed data.

## Architecture

![Alt text](data-flow-architecture-latest.jpeg "flow architecture")

### Components

1. **Cloud Scheduler** – Runs on a defined schedule and publishes a message to a Pub/Sub topic to start the automated process.
2. **Pub/Sub** – Receives the scheduled message and triggers the main serverless Python service.
3. **Cloud Run** - Main Python service responsible for data ingestion and upload.
4. **BigQuery** - Managed, serverless data warehouse for storing raw and transformed data.
5. **Looker Studio** - Visualization layer using BigQuery as the data source.

## Future Enhancements

- Automate infrastructure deployment using Terraform.
- Automate dashboard creating and sharing.
- Implement GitOps process for CICD.
- Integrate with AI layer to create dashboards on demand based on user prompt.
- Alerting, cost control and monitoring.
- Split the main Python function into multiple Cloud Run services.