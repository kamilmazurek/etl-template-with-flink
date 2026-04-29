package template.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.bson.Document;
import template.model.Item;
import template.model.Part;

import java.util.List;
import java.util.stream.Collectors;

public class ItemToDocumentMapper implements MapFunction<Item, Document> {

    @Override
    public Document map(Item item) {
        var document = new Document();

        document.append("_id", item.getId());
        document.append("name", item.getName());
        document.append("description", item.getDescription());
        document.append("parts", extractParts(item));

        return document;
    }

    private List<Document> extractParts(Item item) {
        return item.getParts().stream().map(this::toPartDocument).collect(Collectors.toList());
    }

    private Document toPartDocument(Part part) {
        var document = new Document();

        document.append("partId", part.getPartId());
        document.append("name", part.getName());

        return document;
    }

}
