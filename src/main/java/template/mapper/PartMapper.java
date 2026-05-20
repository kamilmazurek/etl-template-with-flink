package template.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import template.model.Part;

public class PartMapper implements MapFunction<Row, Part> {

    @Override
    public Part map(Row row) {
        return new Part((String) row.getField("id"), (String) row.getField("name"));
    }

}
