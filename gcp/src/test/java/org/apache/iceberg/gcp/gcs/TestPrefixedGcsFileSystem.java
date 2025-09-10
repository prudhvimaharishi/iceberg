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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public class TestPrefixedGcsFileSystem {

  @Test
  public void create_withInvalidParameters_throwsException() {
    assertThatThrownBy(() -> PrefixedGcsFileSystem.create(null, Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid prefix: null or empty");

    assertThatThrownBy(() -> PrefixedGcsFileSystem.create("", Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid prefix: null or empty");

    assertThatThrownBy(() -> PrefixedGcsFileSystem.create("gs://bucket", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
  }

  @Test
  public void create_withValidParams_createsFileSystem() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject");

    PrefixedGcsFileSystem fileSystem = PrefixedGcsFileSystem.create("gs://bucket", properties);

    assertThat(fileSystem).isNotNull();
    assertThat(fileSystem.getPrefix()).isEqualTo("gs://bucket");
    assertThat(fileSystem.getGcsFileSystem()).isNotNull();
  }

  @Test
  public void getCredentials_noOAuthToken_returnsNoCredentials() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject");
    CloseableGroup closeableGroup = new CloseableGroup();

    Credentials credentials = PrefixedGcsFileSystem.getCredentials(properties, closeableGroup);

    assertThat(credentials).isInstanceOf(NoCredentials.class);
  }

  @Test
  public void testClose() throws Exception {
    CloseableGroup mockCloseableGroup = mock(CloseableGroup.class);
    PrefixedGcsFileSystem fileSystem =
        new PrefixedGcsFileSystem(
            "gs://bucket", () -> mock(GcsFileSystem.class), mockCloseableGroup);

    fileSystem.close();

    verify(mockCloseableGroup).close();
  }
}
