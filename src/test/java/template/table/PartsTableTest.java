package template.table;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(SystemStubsExtension.class)
class PartsTableTest {

    @SystemStub
    private EnvironmentVariables env;

    @Test
    void shouldInitializePartsTable() {
        //given environment variables
        env.set("POSTGRESQL_URL", "jdbc:postgresql://localhost:5432/mydb");
        env.set("POSTGRESQL_USER", "testuser");
        env.set("POSTGRESQL_PASSWORD", "testpassword");

        //and Flink environment
        var tableEnv = mock(StreamTableEnvironment.class);
        var tableResultMock = mock(TableResult.class);
        when(tableEnv.executeSql(anyString())).thenReturn(tableResultMock);

        //when parts table is initialized
        PartsTable.init(tableEnv);

        //then code has been executed
        var captor = ArgumentCaptor.forClass(String.class);
        verify(tableEnv).executeSql(captor.capture());
        var code = captor.getValue();

        //and code contains information about parts table
        assertTrue(code.contains("CREATE TABLE parts"));
        assertTrue(code.contains("id STRING"));
        assertTrue(code.contains("item_id STRING"));
        assertTrue(code.contains("name STRING"));

        //and code contains with clause
        assertTrue(code.contains(") WITH ("));
        assertTrue(code.contains("'url' = 'jdbc:postgresql://localhost:5432/mydb'"));
        assertTrue(code.contains("'username' = 'testuser'"));
        assertTrue(code.contains("'table-name' = 'parts'"));
    }

}