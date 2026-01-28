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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJdbcCatalogStore
{
    private JdbcCatalogStore catalogStore;

    @BeforeEach
    public void setUp()
    {
        // Use H2 in-memory database for testing
        JdbcCatalogStoreConfig config = new JdbcCatalogStoreConfig()
                .setConnectionUrl("jdbc:h2:mem:test_catalog_store_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1")
                .setDriverClass("org.h2.Driver");

        catalogStore = new JdbcCatalogStore(config);
    }

    @AfterEach
    public void tearDown()
    {
        // Cleanup is automatic with H2 in-memory database
    }

    @Test
    public void testInitiallyEmpty()
    {
        Collection<CatalogStore.StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).isEmpty();
    }

    @Test
    public void testAddCatalog()
    {
        CatalogName catalogName = new CatalogName("test_catalog");
        ConnectorName connectorName = new ConnectorName("memory");
        Map<String, String> properties = ImmutableMap.of("memory.max-data-per-node", "128MB");

        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(catalogName, connectorName, properties);
        catalogStore.addOrReplaceCatalog(catalogProperties);

        Collection<CatalogStore.StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).hasSize(1);

        CatalogStore.StoredCatalog storedCatalog = catalogs.iterator().next();
        assertThat(storedCatalog.name()).isEqualTo(catalogName);

        CatalogProperties loadedProperties = storedCatalog.loadProperties();
        assertThat(loadedProperties.name()).isEqualTo(catalogName);
        assertThat(loadedProperties.connectorName()).isEqualTo(connectorName);
        assertThat(loadedProperties.properties()).isEqualTo(properties);
    }

    @Test
    public void testReplaceCatalog()
    {
        CatalogName catalogName = new CatalogName("test_catalog");
        ConnectorName connectorName = new ConnectorName("memory");
        Map<String, String> properties1 = ImmutableMap.of("memory.max-data-per-node", "128MB");
        Map<String, String> properties2 = ImmutableMap.of("memory.max-data-per-node", "256MB");

        // Add initial catalog
        CatalogProperties catalogProperties1 = catalogStore.createCatalogProperties(catalogName, connectorName, properties1);
        catalogStore.addOrReplaceCatalog(catalogProperties1);

        // Replace with updated properties
        CatalogProperties catalogProperties2 = catalogStore.createCatalogProperties(catalogName, connectorName, properties2);
        catalogStore.addOrReplaceCatalog(catalogProperties2);

        Collection<CatalogStore.StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).hasSize(1);

        CatalogProperties loadedProperties = catalogs.iterator().next().loadProperties();
        assertThat(loadedProperties.properties()).isEqualTo(properties2);
    }

    @Test
    public void testRemoveCatalog()
    {
        CatalogName catalogName = new CatalogName("test_catalog");
        ConnectorName connectorName = new ConnectorName("memory");
        Map<String, String> properties = ImmutableMap.of("memory.max-data-per-node", "128MB");

        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(catalogName, connectorName, properties);
        catalogStore.addOrReplaceCatalog(catalogProperties);

        assertThat(catalogStore.getCatalogs()).hasSize(1);

        catalogStore.removeCatalog(catalogName);

        assertThat(catalogStore.getCatalogs()).isEmpty();
    }

    @Test
    public void testMultipleCatalogs()
    {
        CatalogName catalog1 = new CatalogName("catalog1");
        CatalogName catalog2 = new CatalogName("catalog2");
        ConnectorName connectorName = new ConnectorName("memory");

        CatalogProperties props1 = catalogStore.createCatalogProperties(catalog1, connectorName, ImmutableMap.of("key1", "value1"));
        CatalogProperties props2 = catalogStore.createCatalogProperties(catalog2, connectorName, ImmutableMap.of("key2", "value2"));

        catalogStore.addOrReplaceCatalog(props1);
        catalogStore.addOrReplaceCatalog(props2);

        assertThat(catalogStore.getCatalogs()).hasSize(2);
    }

    @Test
    public void testReadOnlyStore()
    {
        JdbcCatalogStoreConfig config = new JdbcCatalogStoreConfig()
                .setConnectionUrl("jdbc:h2:mem:test_readonly_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1")
                .setDriverClass("org.h2.Driver")
                .setReadOnly(true);

        JdbcCatalogStore readOnlyStore = new JdbcCatalogStore(config);

        CatalogName catalogName = new CatalogName("test_catalog");
        ConnectorName connectorName = new ConnectorName("memory");

        assertThatThrownBy(() -> readOnlyStore.createCatalogProperties(catalogName, connectorName, ImmutableMap.of()))
                .hasMessageContaining("read only");
    }
}

