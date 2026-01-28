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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestJdbcCatalogStoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcCatalogStoreConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setDriverClass(null)
                .setReadOnly(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:h2:mem:test")
                .put("connection-user", "testuser")
                .put("connection-password", "testpass")
                .put("driver-class", "org.h2.Driver")
                .put("read-only", "true")
                .buildOrThrow();

        JdbcCatalogStoreConfig expected = new JdbcCatalogStoreConfig()
                .setConnectionUrl("jdbc:h2:mem:test")
                .setConnectionUser("testuser")
                .setConnectionPassword("testpass")
                .setDriverClass("org.h2.Driver")
                .setReadOnly(true);

        assertFullMapping(properties, expected);
    }
}

