package template.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import template.model.Item;

public class ItemMapper implements MapFunction<Row, Item> {

    @Override
    public Item map(Row row) {
        var item = new Item();

        item.setId((String) row.getField("id"));
        item.setName((String) row.getField("name"));
        item.setDescription((String) row.getField("description"));

        return item;
    }

}
