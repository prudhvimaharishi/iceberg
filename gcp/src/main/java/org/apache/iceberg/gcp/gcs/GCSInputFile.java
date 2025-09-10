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

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.net.URI;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GCSInputFile extends BaseGCSFile implements InputFile {
  private static final Logger LOG = LoggerFactory.getLogger(GCSInputFile.class);
  private Long blobSize;

  static GCSInputFile fromLocation(
      String location,
      PrefixedStorage storage,
      PrefixedGcsFileSystem prefixedGcsFileSystem,
      MetricsContext metrics) {
    return fromLocation(location, 0L, storage, prefixedGcsFileSystem, metrics);
  }

  static GCSInputFile fromLocation(
      String location,
      long length,
      PrefixedStorage storage,
      PrefixedGcsFileSystem prefixedGcsFileSystem,
      MetricsContext metrics) {
    return new GCSInputFile(
        storage.storage(),
        prefixedGcsFileSystem.getGcsFileSystem(),
        BlobId.fromGsUtilUri(location),
        length > 0 ? length : null,
        storage.gcpProperties(),
        metrics);
  }

  GCSInputFile(
      Storage storage,
      GcsFileSystem gcsFileSystem,
      BlobId blobId,
      Long blobSize,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    super(storage, gcsFileSystem, blobId, gcpProperties, metrics);
    this.blobSize = blobSize;
  }

  @Override
  public long getLength() {
    if (blobSize == null) {
      this.blobSize = getBlob().getSize();
    }

    return blobSize;
  }

  @Override
  public SeekableInputStream newStream() {
    if (gcpProperties().isGcsAnalyticsCoreEnabled()) {
      try {
        return new GoogleCloudStorageInputStreamWrapper(
            GoogleCloudStorageInputStream.create(
                gcsFileSystem(), URI.create(blobId().toGsUtilUri())));
      } catch (IOException e) {
        LOG.warn(
            "Failed to create GCS input stream with gcs-connector, falling back to the default input stream.",
            e);
      }
    }

    return new GCSInputStream(storage(), blobId(), blobSize, gcpProperties(), metrics());
  }
}
