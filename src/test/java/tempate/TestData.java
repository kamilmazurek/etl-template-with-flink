package tempate;

import template.model.Item;
import template.model.Part;

import java.util.List;

public class TestData {

    public static Item createTestItemA() {
        return new Item("item-a", "Item A", "Test item A", List.of(
                new Part("part-a1", "Part A1"),
                new Part("part-a2", "Part A2"),
                new Part("part-a3", "Part A3")
        ));
    }

    public static Item createTestItemB() {
        return new Item("item-b", "Item B", "Test item B", List.of(
                new Part("part-b1", "Part B1"),
                new Part("part-b2", "Part B2")
        ));
    }

    public static Item createTestItemC() {
        return new Item("item-c", "Item C", "Test item C", List.of(
                new Part("part-c1", "Part C1")
        ));
    }

    public static Item createTestItemD() {
        return new Item("item-d", "Item D", "Test item D", List.of());
    }

}
