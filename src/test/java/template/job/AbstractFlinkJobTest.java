package template.job;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(SystemStubsExtension.class)
public abstract class AbstractFlinkJobTest {

    @SystemStub
    protected EnvironmentVariables env;

    protected DataStream<?> dataStream;

    protected SingleOutputStreamOperator<?> singleOutputStreamOperator;

    protected DataStreamSink<?> dataStreamSink;

    protected StreamExecutionEnvironment executionEnv;

    protected StreamTableEnvironment tableEnv;

    private Table table;

    private MockedStatic<StreamExecutionEnvironment> envStatic;

    private MockedStatic<StreamTableEnvironment> tableEnvStatic;

    @BeforeEach
    void setUpFlinkEnvironment() {
        initMocks();
        configureMocks();
        env.set("MONGODB_URI", "mongodb://localhost:27017");
        env.set("MONGODB_DATABASE", "testdb");
    }

    private void initMocks() {
        executionEnv = mock(StreamExecutionEnvironment.class);
        tableEnv = mock(StreamTableEnvironment.class);
        table = mock(Table.class);
        dataStream = mock(DataStream.class);
        singleOutputStreamOperator = mock(SingleOutputStreamOperator.class);
        dataStreamSink = mock(DataStreamSink.class);
        envStatic = mockStatic(StreamExecutionEnvironment.class);
        tableEnvStatic = mockStatic(StreamTableEnvironment.class);
    }

    private void configureMocks() {
        when(executionEnv.setRuntimeMode(BATCH)).thenReturn(executionEnv);
        when(tableEnv.sqlQuery(anyString())).thenReturn(table);
        doReturn(dataStream).when(tableEnv).toDataStream(table);
        doReturn(dataStreamSink).when(singleOutputStreamOperator).sinkTo(any(Sink.class));
        envStatic.when(StreamExecutionEnvironment::getExecutionEnvironment).thenReturn(executionEnv);
        tableEnvStatic.when(() -> StreamTableEnvironment.create(executionEnv)).thenReturn(tableEnv);
    }

    @AfterEach
    void tearDownFlinkEnvironment() {
        envStatic.close();
        tableEnvStatic.close();
    }
}