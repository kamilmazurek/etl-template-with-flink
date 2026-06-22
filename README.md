# ETL Architecture Template with Apache Flink

This repository contains an implementation of a Java-based ETL (Extract, Transform, Load) pipeline designed to migrate and transform data between databases.
Out of the box, it provides a fully working PostgreSQL to MongoDB pipeline that can be adapted to other sources or targets using Flink connectors.
It serves as a ready-to-use template for modern data engineering workloads.

The data pipeline is built using a unified combination of the Flink Table/SQL API and DataStream API,
offering a modular, decoupled structure designed to quickly bootstrap scalable, fault-tolerant ETL applications.

Key advantages:
* **Developer Productivity**: Ready-to-use structure eliminates boilerplate job, environment, schema, and connector configurations.
* **Separation of Concerns**: Joins are handled in Flink SQL, domain mappings occur in isolated mappers, and data delivery is handled by dedicated sinks.
* **Performance & Efficiency**: Execution engine optimizes memory usage and pipelined transformations out of the box.
* **Flexibility**: Changing input sources or output destinations requires fewer changes to your core data processing logic.
* **Ease of Testing**: Modular design enables straightforward operator unit testing and end-to-end integration testing.

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
5. Run ETL job by sending a POST request to Flink’s native REST API endpoint, e.g.:
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