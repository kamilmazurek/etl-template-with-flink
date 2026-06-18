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
import org.mockito.MockedStatic;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public abstract class AbstractFlinkJobTest {

    protected DataStream<?> dataStream;

    protected SingleOutputStreamOperator<?> singleOutputStreamOperator;

    protected DataStreamSink<?> dataStreamSink;

    protected StreamExecutionEnvironment env;

    protected StreamTableEnvironment tableEnv;

    private Table table;

    private MockedStatic<StreamExecutionEnvironment> envStatic;

    private MockedStatic<StreamTableEnvironment> tableEnvStatic;

    @BeforeEach
    void setUpFlinkEnvironment() {
        initMocks();
        configureMocks();
    }

    private void initMocks() {
        env = mock(StreamExecutionEnvironment.class);
        tableEnv = mock(StreamTableEnvironment.class);
        table = mock(Table.class);
        dataStream = mock(DataStream.class);
        singleOutputStreamOperator = mock(SingleOutputStreamOperator.class);
        dataStreamSink = mock(DataStreamSink.class);
        envStatic = mockStatic(StreamExecutionEnvironment.class);
        tableEnvStatic = mockStatic(StreamTableEnvironment.class);
    }

    private void configureMocks() {
        when(env.setRuntimeMode(BATCH)).thenReturn(env);
        when(tableEnv.sqlQuery(anyString())).thenReturn(table);
        doReturn(dataStream).when(tableEnv).toDataStream(table);
        doReturn(dataStreamSink).when(singleOutputStreamOperator).sinkTo(any(Sink.class));
        envStatic.when(StreamExecutionEnvironment::getExecutionEnvironment).thenReturn(env);
        tableEnvStatic.when(() -> StreamTableEnvironment.create(env)).thenReturn(tableEnv);
    }

    @AfterEach
    void tearDownFlinkEnvironment() {
        envStatic.close();
        tableEnvStatic.close();
    }
}