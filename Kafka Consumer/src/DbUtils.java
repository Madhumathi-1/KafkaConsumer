import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;

public class DbUtils {
    // Database connection details
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/KafkaDBConnect";
    private static final String DB_USER = "madhumathi";
    private static final String DB_PASSWORD = "mad@1";

    // Setting up the data source using Apache Commons DBCP
    public static final BasicDataSource dataSource = setupDataSource();
    public static final DbUtils instance = new DbUtils();

    public static Connection connection;

    public DbUtils() {
        try {
            DbUtils.connection = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DbUtils getInstance() {
        return instance;
    }

    private static BasicDataSource setupDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(JDBC_URL);
        ds.setUsername(DB_USER);
        ds.setPassword(DB_PASSWORD);
        ds.setMinIdle(5);
        ds.setMaxIdle(10);
        ds.setMaxOpenPreparedStatements(100);
        return ds;
    }

    // Inserting the data into the database
    public static int performInsert(String tableName, List<String> columnNames, List<String> values) throws Exception {
        try {
            StringBuilder columnNamesString = new StringBuilder();
            StringBuilder valuesString = new StringBuilder();
            int numOfColumns = columnNames.size();

            for (int i = 0; i < numOfColumns; i++) {
                columnNamesString.append(columnNames.get(i)).append(",");
                valuesString.append("?,");
            }

            columnNamesString.deleteCharAt(columnNamesString.length() - 1);
            valuesString.deleteCharAt(valuesString.length() - 1);

            String query = "INSERT INTO " + tableName + " (" + columnNamesString.toString() + ") VALUES ("
                    + valuesString.toString() + ")";
            try (PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

                for (int i = 0; i < numOfColumns; i++) {
                    statement.setString(i + 1, values.get(i));
                }
                statement.executeUpdate();

                ResultSet generatedKeys = statement.getGeneratedKeys();
                // id returns
                if (generatedKeys.next()) {
                    return generatedKeys.getInt(1);
                }
            }
        } catch (Exception ee) {
            ee.printStackTrace();
        }
        return 0;
    }

    // Updating the data in the database
    public static int performUpdate(String tableName, List<String> columnNames, List<String> values,
            List<String> whereConditions) {
        try {
            StringBuilder updateQuery = new StringBuilder("UPDATE ");
            updateQuery.append(tableName).append(" SET ");

            int numColumns = columnNames.size();
            for (int i = 0; i < numColumns; i++) {
                updateQuery.append(columnNames.get(i)).append(" = ?");
                if (i < numColumns - 1) {
                    updateQuery.append(", ");
                }
            }
            // WHERE conditions
            if (!whereConditions.isEmpty()) {
                updateQuery.append(" WHERE ");
                updateQuery.append(whereConditions.stream().collect(Collectors.joining(" AND ")));
            }

            try (PreparedStatement updateStatement = connection.prepareStatement(updateQuery.toString())) {
                int parameterIndex = 1;
                for (int i = 0; i < numColumns; i++) {
                    updateStatement.setString(parameterIndex++, values.get(i));
                }

                int rowsUpdated = updateStatement.executeUpdate();
                return rowsUpdated;
            }
        } catch (Exception ee) {
            ee.printStackTrace();
            return 0;
        }
    }

    // Selecting data with conditions and limit
    public static String performSelect(List<String> columnNames, String tableName, List<String> whereConditions, int limit) throws Exception {
        StringBuilder selectQuery = new StringBuilder("SELECT ");
        
        if (columnNames.isEmpty()) {
            selectQuery.append("*");
        } else {
        	 selectQuery.append(String.join(", ", columnNames));
        }

        selectQuery.append(" FROM ").append(tableName);

        if (whereConditions != null && !whereConditions.isEmpty()) {
            selectQuery.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        selectQuery.append(" LIMIT ?");

        try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString())) {
            int parameterIndex = 1;

//            if (whereConditions != null) {
//                for (String condition : whereConditions) {
//                    preparedStatement.setString(parameterIndex++, condition);
//                }
//            }

            preparedStatement.setInt(parameterIndex, limit);

             ResultSet resultSet = preparedStatement.executeQuery();
                StringBuilder result = new StringBuilder();
                while (resultSet.next()) {
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        result.append(resultSet.getString(i));
                    }
                }
                return result.toString();
            }catch(Exception ee) {
            	System.out.println("Error! "+ee.getMessage());
            	ee.printStackTrace();
            }
		return null;
        
    }
    
    // Deleting data in the database
    public static String performDelete(String tableName, List<String> whereConditions) {
        try {
            StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
            deleteQuery.append(tableName);

            if (!whereConditions.isEmpty()) {
                deleteQuery.append(" WHERE ").append(whereConditions.stream().collect(Collectors.joining(" AND ")));
            }

            try (PreparedStatement deleteStatement = connection.prepareStatement(deleteQuery.toString())) {
                int parameterIndex = 1;
                for (String condition : whereConditions) {
                    deleteStatement.setString(parameterIndex++, condition);
                }

                int rowsDeleted = deleteStatement.executeUpdate();

                if (rowsDeleted > 0) {
                    return "Successfully Deleted...";
                } else {
                    return "No matching records found...";
                }
            }
        } catch (Exception ee) {
            ee.printStackTrace();
        }
		return null;

    }

    public static void closeConnection(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
