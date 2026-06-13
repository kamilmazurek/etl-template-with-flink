package tempate.job;

import org.junit.jupiter.api.Test;
import tempate.AbstractIT;
import template.job.ItemsEtlJob;
import template.model.Item;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static tempate.TestData.*;
import static tempate.TestUtils.getMongoCollection;

public class ItemsEtlJobIT extends AbstractIT {

    @Test
    void shouldRunItemsEtlJob() throws Exception {
        //given data in SQL database
        createTestData();

        //when ETL is run
        ItemsEtlJob.main(new String[]{});

        //then transformed data has been put to MongoDB
        var items = getMongoCollection(MONGO.getReplicaSetUrl(), "test_mongo_db", "items", Item.class);

        var expectedItems = List.of(
                createTestItemA(),
                createTestItemB(),
                createTestItemC(),
                createTestItemD()
        );

        assertThat(items)
                .usingRecursiveComparison()
                .ignoringCollectionOrder()
                .isEqualTo(expectedItems);
    }

    private void createTestData() throws SQLException, IOException, URISyntaxException {
        try (var connection = getPostgresConnection(); var statement = connection.createStatement()) {
            var items = readFromFile("sql/02_items.sql");
            var parts = readFromFile("sql/03_parts.sql");
            statement.execute(items);
            statement.execute(parts);
        }
    }

}
