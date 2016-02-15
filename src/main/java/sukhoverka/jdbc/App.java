package sukhoverka.jdbc;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

public class App {
    public static void main(String[] args) throws SQLException, PropertyVetoException {

        TableCreator tableCreator = new TableCreator();
        tableCreator.createUsersSequence();
        tableCreator.createUsersTable();
        tableCreator.createFriendshipsTable();
        tableCreator.createPostsTable();
        tableCreator.createLikesTable();
        

        Dao dao = new Dao();
        dao.insertUsers();
        dao.insertFriendships();
        dao.insertLikes();
        dao.selectLuckyPersons()
                .stream()
                .forEach(System.out::println);

    }

}
