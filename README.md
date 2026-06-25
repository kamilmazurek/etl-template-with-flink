# ETL Architecture Template with Apache Flink

This repository contains an implementation of a Java-based ETL (Extract, Transform, Load) pipeline designed to migrate and transform data between databases.
Out of the box, it provides a fully working PostgreSQL to MongoDB pipeline that can be adapted to other sources or targets using Flink connectors.
It serves as a ready-to-use template for modern data engineering workloads.

The data pipeline is built using a unified combination of the Flink Table/SQL API and DataStream API,
offering a modular, decoupled structure designed to quickly bootstrap scalable, fault-tolerant ETL applications.

Key advantages:
* **Developer Productivity**: Provides configuration for environments, schemas, transformations, and connectors.
* **Separation of Concerns**: Flink SQL handles joins, mappers process domain logic, and sinks manage delivery.
* **Performance & Efficiency**: Execution engine optimizes memory usage and pipelined transformations.
* **Flexibility**: Changing data sources or destinations requires minimal updates to core processing logic.
* **Ease of Testing**: Modular approach simplifies both unit and end-to-end integration testing.

The goal is to keep it simple, clean and easy to modify.

## Quickstart

Following steps provide a quick way to get started with the template:

1. Ensure a JDK is available to build and run the code. Temurin, based on OpenJDK and available from [adoptium.net](https://adoptium.net/), can be used for this purpose.
2. Download the source code either by cloning the repository with Git or by downloading the ZIP file. If you downloaded the ZIP, extract it. Then navigate to the etl-template-with-flink folder.
3. Build the project and compile the code into a JAR file:
    ```shell
    mvnw clean install
    ```
4. Build Docker Image and start the required infrastructure (Flink cluster, PostgreSQL, and MongoDB) using Docker Compose:
    ```shell
    docker compose up --build
    ```
5. Run ETL job by sending a POST request to Flink's native REST API endpoint, e.g.:
    ```shell
    curl -X POST http://localhost:8081/jars/00000000-0000-0000-0000-000000000000_etl-template-with-flink.jar/run?entry-class=template.job.ItemsEtlJob
    ```
6. Verify the processing results by checking your local MongoDB instance. Transformed data should be loaded into the `items` collection. MongoDB connection string:
    ```console
    mongodb://admin:etl-template-mongo-password@localhost:27017/
    ```
7. Modify the source code to fit your needs, rebuild the project, and run the application 🚀.

## Table of Contents

* [Why This Template?](#why-this-template)
* [Architecture Overview](#architecture-overview)
* [Apache Flink as Batch Engine](#apache-flink-as-batch-engine)
* [When to Use ETL Architecture](#when-to-use-etl-architecture)
* [Technology Stack](#technology-stack)
* [How It Works](#how-it-works)
* [Build and Deployment](#build-and-deployment)
* [End to End Flow](#end-to-end-flow)
* [Flink Dashboard and REST API](#flink-dashboard-and-rest-api)
* [Tests](#tests)
* [Additional Resources](#additional-resources)
* [Author](#author)
* [Disclaimer](#disclaimer)

## Why This Template?

My main motivation for creating this project was to have a reusable implementation of an ETL pipeline based on Apache Flink.
Starting a new project often involves repeatedly setting up the same project structure, configuration, database connectors, and tooling.
This template reduces that overhead by providing a solid foundation for building data processing jobs.

To accelerate development while maintaining quality standards, the template is preconfigured with:
* **Apache Flink**
* **PostgreSQL** as the source database
* **MongoDB** as the target database
* **Preconfigured ETL job**
* **Docker support**
* **Unit tests**
* **Integration tests**
* **Allure reports**

It reduces repetitive setup by providing a ready-to-use project structure, allowing developers to focus on data transformation and business requirements.

## Architecture Overview

The core architectural pattern of an ETL (Extract, Transform, Load) pipeline focuses on decoupling data-related operations:
* **Extract**: Reading the data from the source database
* **Transform**: Changing and cleaning the data structure
* **Load**: Saving the data into the target database

This template structures these distinct phases into clear building blocks using a combination of Flink's Table/SQL API and DataStream API, so that a change in your data source or target storage does not cascade through your core processing logic.

The architecture typically consists of Source Tables, Relational SQL Queries, Domain Mappers, and Target Sinks. It cleanly separates declarative relational operations (such as joins and array aggregations) from programmatic object manipulations (such as mappings and domain object nesting).

The image below shows the concept implemented in this project:

### TODO: add image

Apache Flink is well-suited for this architecture due to its unified execution model.
In this template, the pipeline reads from a relational environment, uses Flink's SQL execution planner to handle entity relationships, and then transitions to the DataStream layer to map Java domain representations before pushing documents downstream.

As a result, this project contains a template implementation of a batch ETL pipeline, written in Java with Apache Flink.
The implementation is designed to be modular, flexible, and easy to extend. It consists of:
* **Extraction (Source)**
    * Extracting data from PostgreSQL tables using the Flink Table API
* **Transformation (Core Logic)**
    * Transforming data using Flink SQL
    * Mapping data using a dedicated domain mapper
* **Loading (Sink)**
    * Saving data to MongoDB using the MongoDB Sink

By default, this template extracts data from PostgreSQL and loads documents to MongoDB. However, because each part is isolated, swapping a relational database for a message broker or changing the target document store can be done without rewriting your core mapping logic.

## Apache Flink as Batch Engine

Apache Flink serves as the core execution engine for this template.
Even though Flink is mostly known for real-time stream processing, it features a unified architecture that handles both batch and streaming workloads.
Flink provides a way to use [BATCH execution mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/execution_mode/) out of the box.
This template uses Flink to process finite datasets efficiently, making it a great fit for scheduled ETL jobs, periodic data loads, and one-off data migrations.

Using Flink for batch ETL scales well for larger workloads.
During heavy relational operations like the nested SQL join in this template, Flink's execution engine actively utilizes [managed memory](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_setup_tm/#managed-memory).
If processing datasets exceeds managed memory, Flink can gracefully [spill data to disk](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_tuning/#configure-memory-for-batch-jobs). This helps reduce the risk of the out-of-memory errors that may affect simpler data processing scripts.

Furthermore, Flink's unified [Table and SQL API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/) allows us to use declarative SQL to resolve relational nesting logic, and then bridge to the [DataStream API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/) for custom domain mapping and delivery to MongoDB.
This approach keeps the batch pipeline modular and easy to maintain.

While this template comes preconfigured with a MongoDB sink, Flink supports a broad ecosystem of [built-in connectors](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/overview/).
Because the extraction and transformation steps are isolated, you can change the target destination without rewriting your logic.
You can configure the pipeline to insert the data into a traditional SQL database, stream the events directly into a Kafka topic, or write the data to standard file formats like CSV or Parquet.

## When to Use ETL Architecture

ETL (Extract, Transform, Load) architecture helps separate data operations from core application logic and storage details.
This improves modularity and makes it easier to adapt pipelines as data sources and targets evolve.

This architecture is especially useful when you need to handle complex data transformations.
It makes adjusting to new data formats simpler, especially if you need to:
* **Decouple logic from storage**: Keep transformation logic completely independent of specific database technologies.
* **Simplify data mapping**: Transform complex data to fit a clean, well-defined target schema.
* **Combine multiple data sources**: Merge data from different systems into a unified model.
* **Isolate system changes**: Swap data sources or destinations without rewriting core processing logic.

Thanks to its clear domain mapping, this template pairs naturally with structured application patterns.
For example, it can be used to prepare and load data for services built on [Hexagonal Architecture](https://kamilmazurek.pl/hexagonal-architecture-template) or [Layered Architecture](https://kamilmazurek.pl/layered-architecture-template).
Furthermore, because Flink natively bridges batch and stream processing, you can configure the sink to emit domain events, making it a data ingestion engine for an [Event-Driven Architecture](https://kamilmazurek.pl/event-driven-architecture-template).

However, for simple data copying or mirroring identical tables between databases, an ETL pipeline introduces unnecessary overhead. In those cases, a direct replication tool or database link is usually easier to implement and maintain.

Ultimately, choosing an ETL architecture depends on your data's structural complexity. It is a good fit for applications that need to cleanly transform and move data across different database ecosystems over time, providing a pipeline that is resilient, testable, and easy to evolve.

## Technology Stack

The application is implemented in Java with Apache Flink, and integrates with a relational database (PostgreSQL) and a document store (MongoDB).

This setup is well-suited for handling structured business data and transforming it into document layouts.
It remains easy to switch to other storage systems if needed, since the project uses decoupled components.

The data processing pipeline combines Flink's Table API and SQL with the DataStream API to handle relational nesting and custom mapping seamlessly.
Additionally, Flink's unified execution model simplifies the process of running this pipeline as a regular batch job.
The project infrastructure is orchestrated using Docker Compose, allowing you to spin up the required databases instantly for local development and testing.

In summary, the stack looks as follows:
- **Language and Framework**
    - **Java 17** - Main programming language used to implement the pipeline and domain logic
    - **Apache Flink** - Core stream and batch processing framework used as the execution engine

- **Databases**
    - **PostgreSQL** - Relational source database containing the raw entity tables
    - **MongoDB** - Document target database storing the nested, aggregated outputs
    - **Flink Connectors** - Built-in JDBC and MongoDB integrations used to bridge Flink with the databases

- **Testing**
    - **JUnit** - Framework for writing unit and integration tests for transformation logic
    - **Mockito** - Mocking framework used to isolate components during unit testing
      Testcontainers - Library used to instantiate real PostgreSQL and MongoDB instances dynamically during integration tests
    - **Allure Report** - Tool for generating clear and detailed test execution reports

- **Build & Infrastructure**
    - **Apache Maven** - Build automation tool used for managing dependencies and packaging the job
    - **Docker and Docker Compose** - Platforms for orchestrating local containers to run database instances consistently

This technology stack provides a solid foundation for building and maintaining scalable, high-performance ETL pipelines with decoupled data operations.
Each component was chosen to balance simplicity, structural boundary isolation, and robust processing functionality.

## Disclaimer

THIS SOFTWARE AND ANY DOCUMENTATION INCLUDED IN THIS REPOSITORY AND CREATED BY THE AUTHOR
(INCLUDING, BUT NOT LIMITED TO, THE README.MD FILE) ARE PROVIDED FOR EDUCATIONAL PURPOSES ONLY.

THE SOFTWARE AND DOCUMENTATION ARE PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE,
THE DOCUMENTATION, OR THE USE OR OTHER DEALINGS IN THE SOFTWARE OR DOCUMENTATION.

THIRD-PARTY LIBRARIES REFERENCED OR INCLUDED IN THIS SOFTWARE ARE SUBJECT TO THEIR OWN LICENSES.
THIRD-PARTY DOCUMENTATION OR EXTERNAL RESOURCES REFERENCED IN THIS REPOSITORY ARE SUBJECT TO THEIR OWN LICENSES AND TERMS.

Apache, Apache Flink, and Flink are trademarks of the Apache Software Foundation.
Postgres, PostgreSQL, and the Slonik Logo are trademarks or registered trademarks of the PostgreSQL Community Association of Canada.
MongoDB is a registered trademark of MongoDB, Inc.
Oracle, Java, MySQL, and NetSuite are registered trademarks of Oracle and/or its affiliates. Other names may be trademarks of their respective owners.