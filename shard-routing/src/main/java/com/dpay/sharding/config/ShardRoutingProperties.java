package com.dpay.sharding.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.datasource")
public class ShardRoutingProperties {

    private DatabaseConnectionProperties control = new DatabaseConnectionProperties();
    private Map<String, DatabaseConnectionProperties> shards = new LinkedHashMap<>();

    public DatabaseConnectionProperties getControl() {
        return control;
    }

    public void setControl(DatabaseConnectionProperties control) {
        this.control = control;
    }

    public Map<String, DatabaseConnectionProperties> getShards() {
        return shards;
    }

    public void setShards(Map<String, DatabaseConnectionProperties> shards) {
        this.shards = shards;
    }

    public static class DatabaseConnectionProperties {
        private String jdbcUrl;
        private String username;
        private String password;
        private String driverClassName = "org.postgresql.Driver";

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDriverClassName() {
            return driverClassName;
        }

        public void setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
        }
    }
}
