package template.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import template.model.Item;
import template.model.Part;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class ItemMapper implements MapFunction<Row, Item> {

    @Override
    public Item map(Row row) {
        var item = new Item();

        item.setId((String) row.getField("id"));
        item.setName((String) row.getField("name"));
        item.setDescription((String) row.getField("description"));

        var partsMultiset = (Map<Row, Integer>) row.getField("parts");

        var parts = partsMultiset.keySet().stream()
                .filter(partRow -> partRow.getField("part_id") != null)
                .map(partRow -> new Part((String) partRow.getField("part_id"), (String) partRow.getField("name")))
                .collect(toList());

        item.setParts(parts);
        return item;
    }

}
