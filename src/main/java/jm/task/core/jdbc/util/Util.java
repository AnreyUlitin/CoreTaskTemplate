package jm.task.core.jdbc.util;
import java.sql.*;


public class Util {
    // реализуйте настройку соеденения с БД //jdbc:mysql://localhost:3306/task_1.1.3//

    private static final String URL = "jdbc:mysql://localhost:3306/task_1.1.3";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "12345678";

        public static Connection getConnection () throws SQLException {
            Connection connection = null;
            try {
                Driver driver = new com.mysql.cj.jdbc.Driver();
                DriverManager.registerDriver(driver);
                connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
                System.out.println("Соединение установленно");
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Ошибка соединения");
            }
            return connection;
        }
    }


//в Util::getConnection() у получаемого соединения отключи глобально autoCommit - отключил с помощью connection.setAutoCommit(false);.




