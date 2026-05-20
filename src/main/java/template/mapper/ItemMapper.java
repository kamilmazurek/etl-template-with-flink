package template.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import template.model.Item;
import template.model.Part;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public class ItemMapper implements MapFunction<Row, Item> {

    private final PartMapper partMapper = new PartMapper();

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
        return Optional.ofNullable((Map<Row, Integer>) row.getField("parts"))
                .orElse(emptyMap())
                .keySet()
                .stream()
                .map(partMapper::map)
                .collect(toList());
    }

}
