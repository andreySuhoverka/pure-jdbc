package sukhoverka.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Dao {

    private ComboPooledDataSource dataSource;
    
    public Dao() throws PropertyVetoException {
        dataSource = DataSourceFactory.getDataSource();        
    }
    
    public void insertUsers() throws SQLException {
        try(Connection cn = dataSource.getConnection();
            PreparedStatement ps = cn.prepareStatement("INSERT INTO USERS (ID, NAME, SURNAME, BIRTHDAY) VALUES(USERS_SEQUENCE.NEXTVAL, ?, ?, ?)")
        ) {
            for(int k = 0; k < 1000; k++) {
                ps.setString(1,"richard" + Math.random());
                ps.setString(2,"branson"  + Math.random());
                ps.setDate(3,new java.sql.Date(new Date().getTime()));
                ps.addBatch();                
            }
            ps.executeBatch();
        }
    }
    
    public void insertFriendships() throws SQLException {
        try(Connection cn = dataSource.getConnection();
            PreparedStatement ps = cn.prepareStatement("INSERT INTO FRIENDSHIPS (USER_ID1, USER_ID2, TIMESTAMP) VALUES(?, ?, ?)")
        ) {
            for(int k = 0; k < 150; k++) {
                for(int i = 0; i < 1000; i++) {
                    ps.setInt(1,k);
                    ps.setInt(2,i);
                    ps.setTimestamp(3, new Timestamp(new Date().getTime()));
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }        
    }


    public void insertLikes() throws SQLException {
        try(Connection cn = dataSource.getConnection();
            PreparedStatement ps = cn.prepareStatement("INSERT INTO LIKES (POST_ID, USER_ID, TIMESTAMP) VALUES(?, ?, ?)")
        ) {
            for(int k = 0; k < 1000; k++) {
                for(int i = 0; i < 300; i++) {
                    ps.setInt(1,i);
                    ps.setInt(2,k);
                    Calendar calendar = Calendar.getInstance();
                    calendar.set(Calendar.YEAR, 2015);
                    calendar.set(Calendar.MONTH, 3);                    
                    ps.setTimestamp(3, new Timestamp(calendar.getTime().getTime()));
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }        
    }
    
    public List<String> selectLuckyPersons() throws SQLException {
        List<String> luckyPersons = new ArrayList<>();
        try(Connection cn = dataSource.getConnection();
            PreparedStatement ps = cn.prepareStatement("SELECT * from USERS join FRIENDSHIPS on USERS.ID = FRIENDSHIPS.USER_ID1") // todo: write sql query here
        ) {
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                luckyPersons.add(rs.getString("NAME"));                
            }

        }
        return luckyPersons;
    }
}
