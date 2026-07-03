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
    curl -X POST http://localhost:8081/jars/etl-template-with-flink.jar/run?entry-class=template.job.ItemsEtlJob
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

## How It Works

This implementation follows structured ETL (Extract, Transform, Load) principles by clearly separating different phases of the data processing pipeline to maintain clean boundaries and improve flexibility.
The key stages include table registration, relational SQL transformation, domain mapping, and sink persistence, which are orchestrated within a unified batch execution context.

To explain how these parts work together, let's walk through the pipeline initialization and data flow triggered when executing `ItemsEtlJob`.
We start at the pipeline configuration environment, extract the source tables, execute the reshaping query, map the relational rows into domain objects, and finally persist the aggregated documents into MongoDB.

The job execution begins in `ItemsEtlJob`, which configures a batch execution environment and sets up the Flink Table infrastructure:
```java
public class ItemsEtlJob {

    public static void main(String[] args) throws Exception {
        var streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setRuntimeMode(BATCH);
        var streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);

        new ItemsTable().init(streamTableEnvironment);
        new PartsTable().init(streamTableEnvironment);

        (...)
    }

}
```

The data extraction phase relies on table abstractions like `ItemsTable` and `PartsTable`.
These classes encapsulate the relational schemas and configurations needed to register physical storage views within Flink's runtime environment:

```java
public final class ItemsTable extends Table {

    private static final String ITEMS_TABLE = """
            CREATE TABLE items (
                id STRING,
                name STRING,
                description STRING
            ) WITH (
                %s
            )
            """;

    public ItemsTable() {
        super("items", ITEMS_TABLE);
    }

}
```

Once the underlying source tables are initialized, the transformation phase leverages Flink SQL to handle relational nesting and joins.
This allows the system to aggregate related rows from distinct schemas directly within the processing engine:

```java
var resultTable = streamTableEnvironment.sqlQuery("""
    SELECT
        i.id,
        i.name,
        i.description,
        (
            SELECT COLLECT(
                CAST(ROW(p.id, p.name) AS ROW<id STRING, name STRING>)
            )
            FROM parts p
            WHERE p.item_id = i.id
        ) AS parts
    FROM items i
    """);
```

After the SQL engine generates the nested structural views, the resulting table is converted back into a data stream.
Processing continues via `ItemMapper`, which transforms generic Flink `Row` structures into strongly-typed Java domain models, ensuring that infrastructure rows are isolated from domain definitions:

```java
public class ItemMapper implements MapFunction<Row, Item> {

    private final PartMapper partMapper = new PartMapper();

    @Override
    public Item map(Row row) {
        var item = new Item();

        item.setId((String) row.getField("id"));
        item.setName((String) row.getField("name"));
        item.setDescription((String) row.getField("description"));
        item.setParts(extractParts(row));

        return item;
    }

    (...)
}
```

Finally, the loading phase takes the mapped domain objects and routes them to a target database sink.
The MongoDB infrastructure logic is abstracted behind `MongoSinkProvider`, which handles connection parameter parsing and configures the serialization strategy for document persistence:

```java
public class MongoSinkProvider<T> {

    public MongoSink<T> create(String collectionName) {
        try (var connectionParameters = new MongodbConnectionParameters()) {
            return MongoSink.<T>builder()
                    .setUri(connectionParameters.getUri())
                    .setDatabase(connectionParameters.getDatabase())
                    .setCollection(collectionName)
                    .setSerializationSchema(new UpsertMongoSerializationSchema<>())
                    .build();
        }
    }
}
```

To construct the actual data flow, these processing stages are assembled into the pipeline that converts the SQL result into a data stream, applies the mapper, and attaches the MongoDB sink:
```java
streamTableEnvironment.toDataStream(resultTable)
    .map(new ItemMapper())
    .sinkTo(new MongoSinkProvider<Item>().create("items"))
    .name("ItemsETL output");
```

At this point, the fully configured pipeline is ready to be executed:
```java
streamExecutionEnvironment.execute("ItemsETL");
```

By following these principles, the processing pipeline remains highly adaptable to change, allowing new data mappings or infrastructure sinks to be introduced without breaking existing code.
Consequently, updates can be introduced iteratively, step by step, rather than requiring large-scale refactoring.
This approach supports long-term testability and clean, decoupled data pipeline organization.

## Build and Deployment

The project uses Apache Maven to build the application JAR and Docker to containerize both Flink and the ETL job, providing a flexible artifact that can be deployed to local infrastructure or environments like Kubernetes.
To simplify local development and testing, the project comes with a configured Docker Compose setup that helps manage the Flink cluster and databases.

The standard build compiles the project, executes both unit and integration tests, and installs the JAR file into the local repository:
```shell
mvnw clean install
```

You can use Docker Compose to build the Docker image and deploy the entire application cluster locally, along with all supporting databases, by running the following command:
```shell
docker compose up --build
```

This starts:
* **Apache Flink Job Manager** with dashboard on port 8081
* **Apache Flink Task Manager**
* **PostgreSQL** on port 5432, preloaded with raw source tables for the extraction phase
* **MongoDB** on port 27017, ready to save the processed documents during the loading phase

Database passwords are loaded from the .env file.
**Make sure to change default passwords to keep your environment secure.**

Once the cluster is up, you can monitor the environment via the Apache Flink Dashboard:
```console
http://localhost:8081/
```

The ETL pipeline can then be triggered by submitting a simple POST request to the Flink REST API:
```shell
curl -X POST http://localhost:8081/jars/etl-template-with-flink.jar/run?entry-class=template.job.ItemsEtlJob
```

Alternatively, you can build the Docker image manually without Docker Compose:
```shell
mvnw clean package
docker build -t template/etl-template-with-flink .
```

If you are running this image directly in Docker without Docker Compose, make sure to start a Job Manager and a Task Manager in the same Docker network with the required environment variables.
For configuration details, please see the `docker-compose.yml` file.

## End to End Flow

The project follows a traditional ETL (Extract, Transform, Load) pattern.
It extracts records from a relational source, applies business logic transformations, and persists the results into a document store.

The execution flow looks as follows:
```
POST request -> Flink (Orchestration) -> PostgreSQL (Extract) -> Flink (Transform) -> MongoDB (Load)
```

The pipeline execution is triggered by sending a simple POST request to the Flink REST API, for example:
```shell
curl -X POST http://localhost:8081/jars/etl-template-with-flink.jar/run?entry-class=template.job.ItemsEtlJob
```

The system then processes the data step by step:

**1. Extract (Source: PostgreSQL)**
- Client sends an `HTTP POST` request to the Flink JobManager to run the ItemsEtlJob (from JAR file).
- The pipeline connects to the PostgreSQL instance.

**2. Transform (Engine: Apache Flink)**
- Flink processes the incoming data inside the TaskManager. 
- Data is fetched from relational tables and mapped into internal domain objects.

**3. Load (Sink: MongoDB)**
- The processed objects are converted into database documents.
- The transformed data is emitted to the MongoDB sink, which persists the documents into the target collections.

To verify the initial source data, you can connect to the PostgreSQL instance using the following credentials:
* **Host:** localhost
* **Port:** 5432
* **Database:** testdb
* **Username:** admin
* **Password:** specified in `.env` file (**make sure to change default password to keep your environment secure**)

Once connected, you can see the data by running:
```sql
SELECT * FROM items;
```
```sql
SELECT * FROM parts;
```

To monitor the job execution you can access the Apache Flink Dashboard:
```console
http://localhost:8081/
```
From the dashboard (web interface), you can track metrics, inspect the execution graph, and view running and completed jobs.

To verify that the items were successfully processed, you can check the data directly inside the MongoDB instance:
* **Connection String:** `mongodb://admin:<password-from-env-file>@localhost:27017/`

Password is taken from `.env` file (**make sure to change the default password to keep your environment secure**).

Once connected, you can see the stored documents by running:
```javascript
use local;
db.items.find().pretty();
```

This flow demonstrates how a basic ETL pipeline works in practice with Apache Flink, covering relational data extraction, data transformations, and document store persistence.

## Flink Dashboard and REST API

Apache Flink provides a built-in web dashboard and a REST API for managing, monitoring, and deploying data processing jobs.
By default, these interfaces run on the JobManager node.

The Apache Flink Dashboard can be used to monitor job execution, track metrics, and inspect the execution graph.
You can access the dashboard interface at:
```console
http://localhost:8081/
```

### TODO: add screenshot

By opening this URL in a browser, you can see running and completed jobs on your cluster.
From there, you can view Task Managers and logs, as well as submit new jobs or inspect existing ones.

You can also interact with the cluster programmatically using the Flink REST API.
The template comes with a ready-to-run `ItemsEtlJob`. This job can be run by sending the following `POST` request:
```shell
curl -X POST http://localhost:8081/jars/etl-template-with-flink.jar/run?entry-class=template.job.ItemsEtlJob
```

You should see a response containing the generated Job ID, like:
```json
{
  "jobid":"3c1f1eb86a8e06038f8d9c8d4a21353a"
}
```

To check the job status, the following command can be used:
```shell
curl http://localhost:8081/jobs
```

This command returns a response showing the status of cluster's jobs, like:
```json
{
   "jobs":[
      {
         "id":"3c1f1eb86a8e06038f8d9c8d4a21353a",
         "status":"FINISHED"
      }
   ]
}
```

These interfaces provide a straightforward way to manage your cluster, making it easy to run jobs, check execution states, and view system metrics.

## Tests

The project is covered by both unit and integration tests, with the Maven Surefire Plugin and Maven Failsafe Plugin preconfigured to execute them.

Tests are written using JUnit, Mockito, Testcontainers, and Flink's MiniCluster, covering the transformation functions, connections, and end-to-end pipeline execution.

There are two types of tests implemented in this template:
* **Unit tests** (`*Test.java`) are executed by Surefire and focus on isolated components such as mappers, database configurations, tables, sinks, and job definition.
* **Integration tests** (`*IT.java`) are executed by Failsafe and verify how the entire application interacts with live external components.

The template comes with `AbstractIT`, which can be used to implement integration tests using real PostgreSQL and MongoDB instances running in Docker containers via Testcontainers, alongside Flink's native `MiniClusterExtension`. Therefore, Docker is required to run these tests.
As a practical example, the template includes `ItemsEtlJobIT`, which extends `AbstractIT` to verify the complete end-to-end job execution.

To execute both types of tests, run:
```shell
mvnw clean verify
```

To run only the unit tests via the Maven Surefire Plugin, use:
```shell
mvnw clean test
```

To run the integration tests through the Maven Failsafe Plugin, use:
```shell
mvnw clean integration-test
```
Note: This command will execute the unit tests as well.

Both types of tests are also executed as part of a regular project build:
```shell
mvnw clean install
```

Additionally, Allure Report is preconfigured to generate visual test reports.
To build and view these reports in your browser, run:
```shell
mvnw clean verify
mvnw allure:serve
```

The test report will then open automatically in your browser. An excerpt from the report is shown below:

### TODO add screenshot

This strategy combines unit tests to validate individual transformation logic with integration tests that check the entire data lifecycle.
It ensures that the pipeline reads data from the source database, processes the records, and persists documents to the sink.
This approach works well with the decoupled design of an ETL architecture.

## Additional Resources

* [ETL Architecture Template with Apache Flink](https://kamilmazurek.pl/etl-template-with-apache-flink)
* [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-lts/)
* [Extract, Transform, Load, Wikipedia](https://en.wikipedia.org/wiki/Extract,_transform,_load)
* [Collection Pipeline, Martin Fowler](https://martinfowler.com/articles/collection-pipeline/)
* [Data Lake, Martin Fowler](https://martinfowler.com/bliki/DataLake.html)
* [Apache Flink Testing (with Flink MiniCluster)](https://nightlies.apache.org/flink/flink-docs-lts/docs/dev/datastream/testing/)
* [Testcontainers PostgreSQL Module](https://testcontainers.com/modules/postgresql/)
* [Testcontainers MongoDB Module](https://testcontainers.com/modules/mongodb/)

## Author

This project was created by [Kamil Mazurek](https://kamilmazurek.pl), a Software Engineer based in Warsaw, Poland.
You can also find me on my [LinkedIn profile](https://www.linkedin.com/in/kamil-mazurek).

More of my repositories can also be found on my GitHub and GitLab profiles:
- [Kamil Mazurek on GitHub](https://github.com/kamilmazurek)
- [Kamil Mazurek on GitLab](https://gitlab.com/kamilmazurek)

Thanks for visiting 🙂

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