package template.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@Data
@NoArgsConstructor
public class Item {

    @JsonProperty("_id")
    private String id;

    private String name;

    private String description;

    private List<Part> parts;

}
