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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class JdbcCatalogStoreConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String driverClass;
    private boolean readOnly;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    @ConfigDescription("JDBC connection URL for catalog store database")
    public JdbcCatalogStoreConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    @ConfigDescription("JDBC connection user for catalog store database")
    public JdbcCatalogStoreConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigDescription("JDBC connection password for catalog store database")
    public JdbcCatalogStoreConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    public String getDriverClass()
    {
        return driverClass;
    }

    @Config("driver-class")
    @ConfigDescription("JDBC driver class name")
    public JdbcCatalogStoreConfig setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
        return this;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    @Config("read-only")
    @ConfigDescription("Whether the catalog store is read-only")
    public JdbcCatalogStoreConfig setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
        return this;
    }
}

