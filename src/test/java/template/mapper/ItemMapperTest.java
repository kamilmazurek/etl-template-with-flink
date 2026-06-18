package template.mapper;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import template.model.Part;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ItemMapperTest {

    @Test
    void shouldMapRowToItem() {
        //given mapper
        var mapper = new ItemMapper();

        //and parts
        var part1 = createPartRow("part-a1", "Part A1");
        var part2 = createPartRow("part-a2", "Part A2");
        var part3 = createPartRow("part-a3", "Part A3");

        //and item row containing parts
        var row = createItemRow("item-a", "Item A", "Test Item A", List.of(part1, part2, part3));

        //when mapping is performed
        var item = mapper.map(row);

        //then item fields should match row values
        assertEquals("item-a", item.getId());
        assertEquals("Item A", item.getName());
        assertEquals("Test Item A", item.getDescription());

        //and parts list should contain all parts
        assertEquals(3, item.getParts().size());

        var partIds = item.getParts().stream().map(Part::getPartId).toList();
        assertTrue(partIds.contains("part-a1"));
        assertTrue(partIds.contains("part-a2"));
        assertTrue(partIds.contains("part-a3"));
    }

    @Test
    void shouldMapRowWithoutParts() {
        //given mapper
        var mapper = new ItemMapper();

        //and item row without parts
        var row = createItemRow("item-d", "Item D", "Test Item D", null);

        //when mapping is performed
        var item = mapper.map(row);

        //then parts list should be empty
        assertEquals("item-d", item.getId());
        assertTrue(item.getParts().isEmpty());
    }

    private Row createItemRow(String id, String name, String description, List<Row> parts) {
        var partsMap = Optional.ofNullable(parts).map(list -> list.stream().collect(toMap(part -> part, part -> 1))).orElse(null);
        var row = mock(Row.class);
        when(row.getField("id")).thenReturn(id);
        when(row.getField("name")).thenReturn(name);
        when(row.getField("description")).thenReturn(description);
        when(row.getField("parts")).thenReturn(partsMap);
        return row;
    }

    private Row createPartRow(String id, String name) {
        var row = mock(Row.class);
        when(row.getField("id")).thenReturn(id);
        when(row.getField("name")).thenReturn(name);
        return row;
    }
}