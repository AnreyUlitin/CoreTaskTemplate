package jm.task.core.jdbc;

import com.mysql.cj.util.Util;
import jm.task.core.jdbc.dao.UserDao;
import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        // реализуйте алгоритм здесь

        List<User> users = new ArrayList<>();
        UserServiceImpl userService = new UserServiceImpl();
        userService.createUsersTable();
        userService.saveUser("Томас", "Манн", (byte) 80);
        userService.saveUser("Хулио ", "Кортасар", (byte) 69);
        userService.saveUser("Андрей ", "Платонов", (byte) 51);
        userService.saveUser("Рэй", "Брэдбери", (byte) 91);

        userService.dropUsersTable();
        userService.removeUserById(1);
        userService.getAllUsers();
        userService.cleanUsersTable();

        for (User user : users) {
            System.out.println(user);
        }

    }
}

