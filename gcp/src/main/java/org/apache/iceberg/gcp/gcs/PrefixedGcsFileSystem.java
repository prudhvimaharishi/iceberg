/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.gcp.gcs;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.gcp.GCPAuthUtils;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.SerializableSupplier;

public class PrefixedGcsFileSystem implements AutoCloseable {
    private transient volatile GcsFileSystem gcsFileSystem;
    private final SerializableSupplier<GcsFileSystem> gcsFileSystemSupplier;
    private final String prefix;
    private CloseableGroup closeableGroup;

    public static PrefixedGcsFileSystem create(String prefix, Map<String, String> properties) {
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(prefix), "Invalid prefix: null or empty");
        Preconditions.checkArgument(null != properties, "Invalid properties: null");
        GcsAnalyticsCoreOptions gcsAnalyticsCoreOptions =
                new GcsAnalyticsCoreOptions("", properties);
        GcsFileSystemOptions fileSystemOptions = gcsAnalyticsCoreOptions.getGcsFileSystemOptions();
        CloseableGroup closableGroup = new CloseableGroup();
        Credentials credentials = getCredentials(properties, closableGroup);
        SerializableSupplier<GcsFileSystem> gcsFileSystemSupplier =
                () ->
                        credentials == null
                                ? new GcsFileSystemImpl(fileSystemOptions)
                                : new GcsFileSystemImpl(credentials, fileSystemOptions);
        return new PrefixedGcsFileSystem(prefix, gcsFileSystemSupplier, closableGroup);
    }

    @VisibleForTesting
    PrefixedGcsFileSystem(
            String prefix,
            SerializableSupplier<GcsFileSystem> gcsFileSystemSupplier,
            CloseableGroup closeableGroup) {
        this.prefix = prefix;
        this.gcsFileSystemSupplier = gcsFileSystemSupplier;
        this.closeableGroup = closeableGroup;
    }

    static Credentials getCredentials(
            Map<String, String> properties, CloseableGroup closeableGroup) {
        GCPProperties gcpProperties = new GCPProperties(properties);
        if (gcpProperties.oauth2Token().isPresent()) {
            return GCPAuthUtils.oauth2CredentialsFromGcpProperties(
                    new GCPProperties(properties), closeableGroup);
        } else if (gcpProperties.noAuth()) {
            return NoCredentials.getInstance();
        } else {
            return null;
        }
    }

    public GcsFileSystem getGcsFileSystem() {
        if (gcsFileSystem == null) {
            synchronized (this) {
                if (gcsFileSystem == null) {
                    this.gcsFileSystem = gcsFileSystemSupplier.get();
                }
            }
        }
        return this.gcsFileSystem;
    }

    public String getPrefix() {
        return this.prefix;
    }

    @Override
    public void close() {
        if (null != closeableGroup) {
            try {
                closeableGroup.close();
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }

        if (null != gcsFileSystem) {
            // gcsFileSystem isn't closable, must be closable.
            gcsFileSystem = null;
        }
    }
}
