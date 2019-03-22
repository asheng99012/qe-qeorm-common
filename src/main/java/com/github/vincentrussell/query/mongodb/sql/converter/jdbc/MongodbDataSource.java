package com.github.vincentrussell.query.mongodb.sql.converter.jdbc;

import com.alibaba.druid.pool.DruidAbstractDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.proxy.jdbc.TransactionInfo;
import com.alibaba.druid.stat.JdbcDataSourceStat;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class MongodbDataSource extends DruidAbstractDataSource {
    public String authMechanism;

    public String database;

    public int maxPoolSize;

    public int waitQueueMultiple;

    public String safe;

    public int connectTimeout;


    private int serverSelectionTimeout;

    private String readPreference;


    public String getAuthMechanism() {
        return authMechanism;
    }

    public void setAuthMechanism(String authMechanism) {
        this.authMechanism = authMechanism;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getWaitQueueMultiple() {
        return waitQueueMultiple;
    }

    public void setWaitQueueMultiple(int waitQueueMultiple) {
        this.waitQueueMultiple = waitQueueMultiple;
    }

    public String getSafe() {
        return safe;
    }

    public void setSafe(String safe) {
        this.safe = safe;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }

    public void setServerSelectionTimeout(int serverSelectionTimeout) {
        this.serverSelectionTimeout = serverSelectionTimeout;
    }

    public String getReadPreference() {
        return readPreference;
    }

    public void setReadPreference(String readPreference) {
        this.readPreference = readPreference;
    }
    MongodbConnection mongodbConnection;

    public MongodbDataSource(){
        this(false);
    }
    public MongodbDataSource(boolean lockFair) {
        super(lockFair);
    }

    public void logTransaction(TransactionInfo info) {

    }

    public void setPoolPreparedStatements(boolean value) {

    }

    public long getConnectCount() {
        return 0;
    }

    public long getCloseCount() {
        return 0;
    }

    public long getConnectErrorCount() {
        return 0;
    }

    public int getPoolingCount() {
        return 0;
    }

    public long getRecycleCount() {
        return 0;
    }

    public int getActiveCount() {
        return 0;
    }

    public long getCreateCount() {
        return 0;
    }

    public long getDestroyCount() {
        return 0;
    }

    public List<String> getFilterClassNames() {
        return null;
    }

    public void setMaxActive(int maxActive) {

    }

    public long getRemoveAbandonedCount() {
        return 0;
    }

    public void setConnectProperties(Properties properties) {

    }

    public void handleConnectionException(DruidPooledConnection pooledConnection, Throwable t) throws SQLException {

    }

    @Override
    public void handleConnectionException(DruidPooledConnection conn, Throwable t, String sql) throws SQLException {

    }

    protected void recycle(DruidPooledConnection pooledConnection) throws SQLException {

    }

    public int getActivePeak() {
        return 0;
    }

    public int getRawDriverMajorVersion() {
        return 0;
    }

    public int getRawDriverMinorVersion() {
        return 0;
    }

    public String getProperties() {
        return null;
    }

    public void discardConnection(Connection realConnection) {

    }

    public Connection getConnection() throws SQLException {
        if (mongodbConnection == null) {
            mongodbConnection = new MongodbConnection();
            mongodbConnection.setMongodbDataSource(this);
        }
        return mongodbConnection;
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    public JdbcDataSourceStat getDataSourceStat() {
        return null;
    }
}
