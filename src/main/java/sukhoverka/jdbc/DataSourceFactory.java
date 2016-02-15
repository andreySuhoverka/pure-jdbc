package sukhoverka.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;

public class DataSourceFactory {
    
    public static ComboPooledDataSource dataSource;
    
    public static ComboPooledDataSource getDataSource() throws PropertyVetoException {
        if (dataSource == null) {
            synchronized (DataSourceFactory.class){
                if (dataSource == null) {
                    initDataSource();
                }
            }            
        }        
        return dataSource;
    }

    public static void initDataSource() throws PropertyVetoException {
        dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("oracle.jdbc.driver.OracleDriver");
        dataSource.setJdbcUrl("jdbc:oracle:thin:@localhost:1521");
        dataSource.setUser("andrei");
        dataSource.setPassword("andrei");
        dataSource.setAutoCommitOnClose(true); // due to try with resources syntax, it's not convenient to do commit manually
    }
}
