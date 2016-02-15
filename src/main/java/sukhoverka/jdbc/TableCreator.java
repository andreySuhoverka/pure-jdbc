package sukhoverka.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class TableCreator {
    private String TABLE_USERS = "USERS";
    private String TABLE_FRIENDSHIPS = "FRIENDSHIPS";
    private String TABLE_POSTS = "POSTS";
    private String TABLE_LIKES = "LIKES";

    private String CREATE_TABLE_USERS_SQL = "CREATE TABLE USERS("
            + "ID NUMBER(20) NOT NULL,"
            + "NAME VARCHAR(50) NOT NULL, "
            + "SURNAME VARCHAR(50) NOT NULL, "
            + "BIRTHDAY DATE NOT NULL, "
            + "PRIMARY KEY (ID) "
            + ")";


    private String CREATE_USERS_SEQUENCE_SQL = "CREATE SEQUENCE \"USERS_SEQUENCE\" MINVALUE 1 MAXVALUE 9999999999 INCREMENT BY 1 START WITH 3";

    private String CREATE_TABLE_FRIENDSHIPS_SQL = "CREATE TABLE FRIENDSHIPS("
            + "USER_ID1 NUMBER(10) NOT NULL, "
            + "USER_ID2 NUMBER(10) NOT NULL, "
            + "TIMESTAMP TIMESTAMP NOT NULL, " + "PRIMARY KEY (USER_ID1,USER_ID2) "
            + ")";

    private String CREATE_TABLE_POSTS_SQL = "CREATE TABLE POSTS("
            + "ID NUMBER(10) NOT NULL, "
            + "USER_ID NUMBER(10) NOT NULL, "
            + "TEXT BLOB NOT NULL, "
            + "TIMESTAMP TIMESTAMP NOT NULL, " + "PRIMARY KEY (ID) "
            + ")";

    private String CREATE_TABLE_LIKES_SQL = "CREATE TABLE LIKES("
            + "POST_ID NUMBER(10) NOT NULL, "
            + "USER_ID NUMBER(10) NOT NULL, "
            + "TIMESTAMP TIMESTAMP NOT NULL, " + "PRIMARY KEY (POST_ID,USER_ID) "
            + ")";

    
    private ComboPooledDataSource dataSource;

    public TableCreator() throws PropertyVetoException {
        dataSource = init();
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public ComboPooledDataSource init() throws PropertyVetoException {
        ComboPooledDataSource ds = new ComboPooledDataSource();
        ds.setDriverClass("oracle.jdbc.driver.OracleDriver");
        ds.setJdbcUrl("jdbc:oracle:thin:@localhost:1521");
        ds.setUser("andrei");
        ds.setPassword("andrei");
        return ds;
    }

    public void createUsersTable() throws SQLException {
        createTable(TABLE_USERS, CREATE_TABLE_USERS_SQL);
    }

    public void createUsersSequence() throws SQLException {
        createSequence("USERS_SEQUENCE", CREATE_USERS_SEQUENCE_SQL);
    }

    public void createFriendshipsTable() throws SQLException {
        createTable(TABLE_FRIENDSHIPS, CREATE_TABLE_FRIENDSHIPS_SQL);
    }

    public void createPostsTable() throws SQLException {
        createTable(TABLE_POSTS, CREATE_TABLE_POSTS_SQL);
    }

    public void createLikesTable() throws SQLException {
        createTable(TABLE_LIKES, CREATE_TABLE_LIKES_SQL);
    }

    private void dropTableIfExists(String tableName) throws SQLException {
        try (Connection cn = getConnection();
             PreparedStatement st = cn.prepareStatement(getDropTableSql(tableName))) {
            st.executeUpdate();
        }
    }


    private void dropSequenceIfExists(String sequenceName) throws SQLException {
        try (Connection cn = getConnection();
             PreparedStatement st = cn.prepareStatement(getDropSequenceSql(sequenceName))) {
            st.executeUpdate();
        }
    }

    private String getDropTableSql(String tableName) {
        return "BEGIN\n" +
                "   EXECUTE IMMEDIATE 'DROP TABLE " + tableName + "';\n" +
                "EXCEPTION\n" +
                "   WHEN OTHERS THEN\n" +
                "      IF SQLCODE != -942 THEN\n" +
                "         RAISE;\n" +
                "      END IF;\n" +
                "END;";
    }

    private String getDropSequenceSql(String sequenceName) {
        return "DROP SEQUENCE " + sequenceName;
    }

    private void createTable(String tableName, String createTableSql) throws SQLException {
        dropTableIfExists(tableName);
        try (Connection cn = getConnection();
             PreparedStatement st = cn.prepareStatement(createTableSql)) {
            st.executeUpdate();
        }
    }

    private void createSequence(String sequenceName, String createSequenceSql) throws SQLException {
        dropSequenceIfExists(sequenceName);
        try (Connection cn = getConnection();
             PreparedStatement st = cn.prepareStatement(createSequenceSql)) {
            st.executeUpdate();
        }
    }


}
