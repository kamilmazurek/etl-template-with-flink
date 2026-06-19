package template.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import template.mapper.ItemMapper;
import template.table.ItemsTable;
import template.table.PartsTable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

class ItemsEtlJobTest extends AbstractFlinkJobTest {

    private MockedConstruction<ItemsTable> itemsMockConstruction;

    private MockedConstruction<PartsTable> partsMockConstruction;

    @BeforeEach
    void init() {
        itemsMockConstruction = mockConstruction(ItemsTable.class);
        partsMockConstruction = mockConstruction(PartsTable.class);
    }

    @Test
    void shouldBuildAndExecuteItemsEtlTopology() throws Exception {
        //given mocked domain fluent api behavior
        var typedDataStream = (DataStream<Row>) dataStream;
        doReturn(singleOutputStreamOperator).when(typedDataStream).map(any(ItemMapper.class));

        //when job is executed
        ItemsEtlJob.main(new String[]{});

        //then tables should be initialized
        var itemsTable = itemsMockConstruction.constructed().get(0);
        var partsTable = partsMockConstruction.constructed().get(0);
        verify(itemsTable).init(tableEnv);
        verify(partsTable).init(tableEnv);

        //and sql query should be executed
        verify(tableEnv).sqlQuery(anyString());

        //and sink should be named
        verify(dataStreamSink).name("ItemsETL output");

        //and job should be executed
        verify(executionEnv).execute("ItemsETL");
    }

    @AfterEach
    void cleanup() {
        itemsMockConstruction.close();
        partsMockConstruction.close();
    }
}