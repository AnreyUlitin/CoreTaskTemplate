package jm.task.core.jdbc.dao;

import antlr.*;
import com.mysql.cj.jdbc.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import jm.task.core.jdbc.model.*;
import jm.task.core.jdbc.util.*;

public class UserDaoJDBCImpl implements UserDao {
    private Connection connection;

    public UserDaoJDBCImpl() {
        //пустой конструктор
    }

    //-  вручную закрывай в каждом дао-методе Connection, Statement, ResultSet.
    //- createUsersTable/dropUsersTable-сейчас SQL-запросы сконструированы так,
// чтобы при попытке создания уже существующей таблицы или удаления несуществующей-будет выброшено исключение. Видоизмени запросы в этих методах
    public void createUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS Users " +
                    "(id BIGINT PRIMARY KEY AUTO_INCREMENT, " +
                    "name VARCHAR(46) NOT NULL, " +
                    "lastName VARCHAR(64) NOT NULL, " +
                    "age TINYINT NOT NULL)");
            System.out.println("Создана таблица userdao");
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (SQLException e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    //-  вручную закрывай в каждом дао-методе Connection, Statement, ResultSet.
    //- createUsersTable/dropUsersTable-сейчас SQL-запросы сконструированы так,
// чтобы при попытке создания уже существующей таблицы или удаления несуществующей-будет выброшено исключение. Видоизмени запросы в этих методах
    public void dropUsersTable() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS userdao");
            System.out.println("drop user table");
            statement.close();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (SQLException e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    //- saveUser - id присваивается базой (ты сам указал свойство auto_increment), его не надо пробрасывать в запросе.
    //-  рекомендую здесь отказаться от ресурсного try - так ты сможешь не потерять исключения, выбрасываемые неудачной попыткой close() ресурса

    public void saveUser(String name, String lastName, byte age) {
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement("INSERT INTO usersdao (name, lastname, age) VALUES (?, ?, ?)");
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            connection.commit();
            preparedStatement.close();
            System.out.println("User с имя: " + name + " фамилия: " + lastName + " возраст: " + age + " добавлен в БД");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
    }


//     public void saveUser(String name, String lastName, byte age) {
//         PreparedStatement preparedStatement = null;
//            try (PreparedStatement pst = connection.prepareStatement
//                    ("INSERT INTO users (name, last_name, age) VALUES (?, ?, ?)")) {
//                pst.setString(1, name);
//                pst.setString(2, lastName);
//                pst.setInt(3, age);
//                pst.executeUpdate();
//                connection.commit();
//            } catch (SQLException e) {
//                e.printStackTrace();
//                try {
//                    connection.close();
//                } catch (SQLException e1) {
//                    e1.printStackTrace();
//                }
//                System.out.println("save user table");
//            }
//        }


    public void removeUserById(long id) {
        try {
            Connection connection = Util.getConnection();
            Statement statement = connection.createStatement();
            long byId = statement.executeUpdate("DELETE FROM usersdao WHERE id = " + id);
            System.out.println("Удалён пользователь с id = " + byId);
            connection.commit();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                connection.close();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
        }
    }


    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM usersdao");
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastname"));
                user.setAge(resultSet.getByte("age"));
                users.add(user);
            }
            connection.commit();
            statement.close();
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
        System.out.println("Get all users");
        return users;
    }


    public void cleanUsersTable() {
        PreparedStatement statement = null;
        try {
            Connection connection = Util.getConnection();
            statement = connection.prepareStatement("TRUNCATE TABLE usersdao");
            statement.executeUpdate();
            System.out.println("clean Users Table");
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            connection.rollback();
        } catch (SQLException e1) {
            e1.printStackTrace();
        } finally {
            try {
                assert statement != null;
                statement.close();
            } catch (SQLException e2) {
                e2.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
    }
}


//В проект необходимо добавить транзакционность (почитай об этом понятии и  о принципе ACID).
//Для этого:
//- в дао-методах добавь коммиты и роллбэки операций.
// Учти, и те, и другие способны выбросить исключение.
// Их нужно обработать.
// Не пугайся try внутри try, пусть и кажется лапшой.
// В данной задаче это допустимо, но в пределах разумного.


//-  вручную закрывай в каждом дао-методе Connection, Statement, ResultSet.
// Метод close() cпособен выбросить исключение, его обработать