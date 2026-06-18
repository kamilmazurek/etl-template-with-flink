package template.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SystemStubsExtension.class)
class JdbcConnectorOptionsTest {

    @SystemStub
    private EnvironmentVariables env;

    @Test
    void shouldFormatWithClauseCorrectly() {
        //given environment variables
        env.set("POSTGRESQL_URL", "jdbc:postgresql://localhost:5432/mydb");
        env.set("POSTGRESQL_USER", "testuser");
        env.set("POSTGRESQL_PASSWORD", "testpassword");

        //and jdbc connection parameters
        var params = new JdbcConnectionParameters();
        var tableName = "public.users";

        //when with clause is generated
        var withClause = JdbcConnectorOptions.toWithClause(params, tableName);

        //then with clause should match expected format
        var expectedClause = """
                'connector' = 'jdbc',
                'driver' = 'org.postgresql.Driver',
                'url' = 'jdbc:postgresql://localhost:5432/mydb',
                'username' = 'testuser',
                'password' = 'testpassword',
                'table-name' = 'public.users'
                """;
        assertEquals(expectedClause, withClause);
    }
}