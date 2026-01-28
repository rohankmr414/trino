/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.trino.connector.FileCatalogStore.computeCatalogVersion;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public final class JdbcCatalogStore
        implements CatalogStore
{
    private static final Logger log = Logger.get(JdbcCatalogStore.class);
    private static final String TABLE_NAME = "trino_catalogs";

    private final Jdbi jdbi;
    private final boolean readOnly;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<CatalogName, StoredCatalog> catalogs = new ConcurrentHashMap<>();

    @Inject
    public JdbcCatalogStore(JdbcCatalogStoreConfig config)
    {
        requireNonNull(config, "config is null");
        this.readOnly = config.isReadOnly();
        this.objectMapper = new ObjectMapperProvider().get();

        // Load JDBC driver if specified
        if (config.getDriverClass() != null) {
            try {
                Class.forName(config.getDriverClass());
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load JDBC driver: " + config.getDriverClass(), e);
            }
        }

        // Create JDBI instance
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }

        try {
            Driver driver = DriverManager.getDriver(config.getConnectionUrl());
            this.jdbi = Jdbi.create(() -> driver.connect(config.getConnectionUrl(), connectionProperties));
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create JDBC connection for catalog store", e);
        }

        // Initialize database schema
        initializeSchema();

        // Load existing catalogs
        loadCatalogs();
    }

    private void initializeSchema()
    {
        try (Handle handle = jdbi.open()) {
            handle.useTransaction(transactionHandle -> {
                transactionHandle.execute(
                        "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                                "catalog_name VARCHAR(255) NOT NULL PRIMARY KEY, " +
                                "connector_name VARCHAR(255) NOT NULL, " +
                                "properties TEXT NOT NULL, " +
                                "version VARCHAR(255) NOT NULL, " +
                                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                                ")");

                // Create index for faster lookups by connector type
                transactionHandle.execute(
                        "CREATE INDEX IF NOT EXISTS idx_connector_name ON " + TABLE_NAME + "(connector_name)");
            });
            log.info("Initialized JDBC catalog store schema");
        }
        catch (Exception e) {
            log.error(e, "Failed to initialize catalog store schema");
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to initialize catalog store schema", e);
        }
    }

    private void loadCatalogs()
    {
        try (Handle handle = jdbi.open()) {
            List<Map<String, Object>> rows = handle.inTransaction(transactionHandle ->
                    transactionHandle.createQuery(
                                    "SELECT catalog_name, connector_name, properties, version FROM " + TABLE_NAME)
                            .mapToMap()
                            .list());

            for (Map<String, Object> row : rows) {
                String catalogName = (String) row.get("catalog_name");
                catalogs.put(
                        new CatalogName(catalogName),
                        new JdbcStoredCatalog(
                                new CatalogName(catalogName),
                                (String) row.get("connector_name"),
                                (String) row.get("properties"),
                                (String) row.get("version")));
            }
            log.info("Loaded %d catalogs from JDBC store", catalogs.size());
        }
        catch (Exception e) {
            log.error(e, "Failed to load catalogs from store");
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to load catalogs from store", e);
        }
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return ImmutableList.copyOf(catalogs.values());
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        checkModifiable();
        return new CatalogProperties(
                catalogName,
                computeCatalogVersion(catalogName, connectorName, properties),
                connectorName,
                ImmutableMap.copyOf(properties));
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        checkModifiable();
        CatalogName catalogName = catalogProperties.name();

        try {
            String propertiesJson = objectMapper.writeValueAsString(catalogProperties.properties());
            String version = catalogProperties.version().version();

            try (Handle handle = jdbi.open()) {
                handle.useTransaction(transactionHandle -> {
                    // Use INSERT ... ON CONFLICT for atomic upsert
                    // This syntax is supported by PostgreSQL 9.5+ and H2 2.0+
                    transactionHandle.createUpdate(
                                    "INSERT INTO " + TABLE_NAME + " (catalog_name, connector_name, properties, version, created_at, updated_at) " +
                                            "VALUES (:catalog_name, :connector_name, :properties, :version, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) " +
                                            "ON CONFLICT (catalog_name) DO UPDATE SET " +
                                            "connector_name = EXCLUDED.connector_name, " +
                                            "properties = EXCLUDED.properties, " +
                                            "version = EXCLUDED.version, " +
                                            "updated_at = CURRENT_TIMESTAMP")
                            .bind("catalog_name", catalogName.toString())
                            .bind("connector_name", catalogProperties.connectorName().toString())
                            .bind("properties", propertiesJson)
                            .bind("version", version)
                            .execute();
                });
            }

            catalogs.put(catalogName, new JdbcStoredCatalog(
                    catalogName,
                    catalogProperties.connectorName().toString(),
                    propertiesJson,
                    version));

            log.info("Stored catalog: %s", catalogName);
        }
        catch (JsonProcessingException e) {
            log.error(e, "Failed to serialize catalog properties for %s", catalogName);
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to serialize catalog properties for " + catalogName, e);
        }
        catch (Exception e) {
            log.error(e, "Failed to store catalog properties for %s", catalogName);
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to store catalog properties for " + catalogName, e);
        }
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        checkModifiable();

        try (Handle handle = jdbi.open()) {
            handle.useTransaction(transactionHandle -> {
                int deleted = transactionHandle.createUpdate("DELETE FROM " + TABLE_NAME + " WHERE catalog_name = :catalog_name")
                        .bind("catalog_name", catalogName.toString())
                        .execute();

                if (deleted > 0) {
                    catalogs.remove(catalogName);
                    log.info("Removed catalog: %s", catalogName);
                }
                else {
                    log.debug("Catalog not found in database: %s", catalogName);
                    catalogs.remove(catalogName);
                }
            });
        }
        catch (Exception e) {
            log.error(e, "Failed to remove catalog properties for %s", catalogName);
            throw new TrinoException(CATALOG_STORE_ERROR, "Failed to remove catalog properties for " + catalogName, e);
        }
    }

    private void checkModifiable()
    {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "Catalog store is read only");
        }
    }

    private class JdbcStoredCatalog
            implements StoredCatalog
    {
        private final CatalogName name;
        private final String connectorName;
        private final String propertiesJson;
        private final String version;

        public JdbcStoredCatalog(CatalogName name, String connectorName, String propertiesJson, String version)
        {
            this.name = requireNonNull(name, "name is null");
            this.connectorName = requireNonNull(connectorName, "connectorName is null");
            this.propertiesJson = requireNonNull(propertiesJson, "propertiesJson is null");
            this.version = requireNonNull(version, "version is null");
        }

        @Override
        public CatalogName name()
        {
            return name;
        }

        @Override
        public CatalogProperties loadProperties()
        {
            try {
                @SuppressWarnings("unchecked")
                Map<String, String> properties = objectMapper.readValue(propertiesJson, Map.class);
                return new CatalogProperties(
                        name,
                        new CatalogVersion(version),
                        new ConnectorName(connectorName),
                        ImmutableMap.copyOf(properties));
            }
            catch (IOException e) {
                throw new TrinoException(CATALOG_STORE_ERROR, "Failed to deserialize catalog properties for " + name, e);
            }
        }
    }
}
