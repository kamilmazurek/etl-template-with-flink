package template.mapper;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartMapperTest {

    @Test
    void shouldMapRowToPart() {
        //given mapper
        var mapper = new PartMapper();

        //and row
        var row = mock(Row.class);
        when(row.getField("id")).thenReturn("part-a1");
        when(row.getField("name")).thenReturn("Test Part A1");

        //when mapping is performed
        var part = mapper.map(row);

        //then part fields should match row values
        assertEquals("part-a1", part.getPartId());
        assertEquals("Test Part A1", part.getName());
    }
}