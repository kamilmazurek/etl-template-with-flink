package template.job;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.STRING;

public class ItemsETL {

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var dbUrl = System.getenv("DB_URL");
        var dbUser = System.getenv("DB_USER");
        var dbPassword = System.getenv("DB_PASSWORD");

        var jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDBUrl(dbUrl)
                .setUsername(dbUser)
                .setPassword(dbPassword)
                .setDrivername("org.postgresql.Driver")
                .setQuery("SELECT id, name, description FROM items")
                .setRowTypeInfo(new RowTypeInfo(STRING, STRING, STRING))
                .finish();

        var items = env.createInput(jdbcInputFormat);
        items.print();
        env.execute("ItemsETL");
    }

}