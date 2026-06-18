package template.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import template.mapper.ItemMapper;
import template.sink.Sink;
import template.table.ItemsTable;
import template.table.PartsTable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

class ItemsEtlJobTest extends AbstractFlinkJobTest {

    private MockedStatic<ItemsTable> itemsTableStatic;

    private MockedStatic<PartsTable> partsTableStatic;

    private MockedStatic<Sink> sinkStatic;

    @BeforeEach
    void setUpDomain() {
        itemsTableStatic = mockStatic(ItemsTable.class);
        partsTableStatic = mockStatic(PartsTable.class);
        sinkStatic = mockStatic(Sink.class);
    }

    @Test
    void shouldBuildAndExecuteItemsEtlTopology() throws Exception {
        //given domain mocks
        var mongoSink = mock(org.apache.flink.connector.mongodb.sink.MongoSink.class);

        //and mocked domain fluent api behavior
        var typedDataStream = (DataStream<Row>) dataStream;
        doReturn(singleOutputStreamOperator).when(typedDataStream).map(any(ItemMapper.class));
        sinkStatic.when(() -> Sink.createMongoSink("items")).thenReturn(mongoSink);

        //when job is executed
        ItemsEtlJob.main(new String[]{});

        //then tables should be initialized
        itemsTableStatic.verify(() -> ItemsTable.init(tableEnv));
        partsTableStatic.verify(() -> PartsTable.init(tableEnv));

        //and sql query should be executed
        verify(tableEnv).sqlQuery(anyString());

        //and sink should be named
        verify(dataStreamSink).name("ItemsETL output");

        //and job should be executed
        verify(env).execute("ItemsETL");
    }

    @AfterEach
    void tearDownDomain() {
        itemsTableStatic.close();
        partsTableStatic.close();
        sinkStatic.close();
    }

}