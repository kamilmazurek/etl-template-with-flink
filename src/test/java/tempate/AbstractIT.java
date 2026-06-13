package tempate;

import org.apache.flink.mongodb.shaded.com.mongodb.client.MongoClients;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

@Testcontainers
@ExtendWith(SystemStubsExtension.class)
public abstract class AbstractIT {

    @RegisterExtension
    protected static final MiniClusterExtension FLINK_CLUSTER = new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder().build());

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpassword")
            .withInitScript("sql/01_schema.sql");

    @Container
    protected static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0");

    @SystemStub
    protected static EnvironmentVariables environmentVariables;

    @BeforeAll
    static void setup() {
        environmentVariables.set("POSTGRESQL_URL", POSTGRES.getJdbcUrl());
        environmentVariables.set("POSTGRESQL_USER", POSTGRES.getUsername());
        environmentVariables.set("POSTGRESQL_PASSWORD", POSTGRES.getPassword());
        environmentVariables.set("MONGODB_URI", MONGO.getReplicaSetUrl());
        environmentVariables.set("MONGODB_DATABASE", "test_mongo_db");
    }

    @BeforeEach
    void cleanup() throws Exception {
        try (var connection = getPostgresConnection(); var statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE items, parts CASCADE");
        }

        try (var mongoClient = MongoClients.create(MONGO.getReplicaSetUrl())) {
            mongoClient.getDatabase("test_mongo_db").getCollection("items").drop();
        }
    }

    protected Connection getPostgresConnection() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    protected String readFromFile(String name) throws IOException, URISyntaxException {
        return Files.readString(Path.of(Objects.requireNonNull(getClass().getClassLoader().getResource(name)).toURI()));
    }

}
