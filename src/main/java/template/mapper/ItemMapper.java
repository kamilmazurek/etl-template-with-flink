package template.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import template.model.Item;
import template.model.Part;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ItemMapper implements MapFunction<Row, Item> {

    @Override
    public Item map(Row row) {
        var item = new Item();

        item.setId((String) row.getField("id"));
        item.setName((String) row.getField("name"));
        item.setDescription((String) row.getField("description"));
        item.setParts(extractParts(row));

        return item;
    }

    private List<Part> extractParts(Row row) {
        var partsMultiset = (Map<Row, Integer>) row.getField("parts");

        if (partsMultiset == null) {
            return new ArrayList<>();
        }

        return partsMultiset.keySet().stream()
                .filter(partRow -> partRow != null && partRow.getField("part_id") != null)
                .map(this::toPart)
                .collect(Collectors.toList());
    }

    private Part toPart(Row row) {
        return new Part((String) row.getField("part_id"), (String) row.getField("name"));
    }

}
