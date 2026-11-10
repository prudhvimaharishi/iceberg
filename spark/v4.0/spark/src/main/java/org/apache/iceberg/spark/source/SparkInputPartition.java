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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;

class SparkInputPartition implements InputPartition, HasPartitionKey, Serializable {
  private static final ExecutorService WARMUP_EXECUTOR =
      ThreadPools.newExitingWorkerPool("iceberg-partition-warmup", 8);
  private static final Set<Long> WARMED_BROADCAST_IDS =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final Types.StructType groupingKeyType;
  private final ScanTaskGroup<?> taskGroup;
  private final Broadcast<Table> tableBroadcast;
  private final Broadcast<FileIO> fileIOBroadcast;
  private final String branch;
  private final String expectedSchemaString;
  private final boolean caseSensitive;
  private final transient String[] preferredLocations;
  private final boolean cacheDeleteFilesOnExecutors;
  private final boolean eagerWarmUpEnabled;

  private transient Schema expectedSchema = null;

  SparkInputPartition(
      Types.StructType groupingKeyType,
      ScanTaskGroup<?> taskGroup,
      Broadcast<Table> tableBroadcast,
      Broadcast<FileIO> fileIOBroadcast,
      String branch,
      String expectedSchemaString,
      boolean caseSensitive,
      String[] preferredLocations,
      boolean cacheDeleteFilesOnExecutors) {
    this(
        groupingKeyType,
        taskGroup,
        tableBroadcast,
        fileIOBroadcast,
        branch,
        expectedSchemaString,
        caseSensitive,
        preferredLocations,
        cacheDeleteFilesOnExecutors,
        false);
  }

  SparkInputPartition(
      Types.StructType groupingKeyType,
      ScanTaskGroup<?> taskGroup,
      Broadcast<Table> tableBroadcast,
      Broadcast<FileIO> fileIOBroadcast,
      String branch,
      String expectedSchemaString,
      boolean caseSensitive,
      String[] preferredLocations,
      boolean cacheDeleteFilesOnExecutors,
      boolean eagerWarmUpEnabled) {
    this.groupingKeyType = groupingKeyType;
    this.taskGroup = taskGroup;
    this.tableBroadcast = tableBroadcast;
    this.fileIOBroadcast = fileIOBroadcast;
    this.branch = branch;
    this.expectedSchemaString = expectedSchemaString;
    this.caseSensitive = caseSensitive;
    this.preferredLocations = preferredLocations;
    this.cacheDeleteFilesOnExecutors = cacheDeleteFilesOnExecutors;
    this.eagerWarmUpEnabled = eagerWarmUpEnabled;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    if (eagerWarmUpEnabled
        && fileIOBroadcast != null
        && WARMED_BROADCAST_IDS.add(fileIOBroadcast.id())) {
      Broadcast<FileIO> broadcastRef = fileIOBroadcast;
      WARMUP_EXECUTOR.execute(() -> triggerFileIOWarmUp(broadcastRef));
    }
  }

  private static void triggerFileIOWarmUp(Broadcast<FileIO> broadcastRef) {
    ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(SparkInputPartition.class.getClassLoader());
    try {
      FileIO io = broadcastRef.value();
      if (io instanceof SerializableFileIOWithSize wrapper) {
        io = wrapper.delegate();
      }
      if (io != null) {
        Method method = io.getClass().getMethod("startWarmUpAsync");
        method.invoke(io);
      }
    } catch (ReflectiveOperationException | RuntimeException ignored) {
      // Non-GCS FileIO implementations do not expose startWarmUpAsync.
    } finally {
      Thread.currentThread().setContextClassLoader(previousLoader);
    }
  }

  @Override
  public String[] preferredLocations() {
    return preferredLocations;
  }

  @Override
  public InternalRow partitionKey() {
    return new StructInternalRow(groupingKeyType).setStruct(taskGroup.groupingKey());
  }

  @SuppressWarnings("unchecked")
  public <T extends ScanTask> ScanTaskGroup<T> taskGroup() {
    return (ScanTaskGroup<T>) taskGroup;
  }

  public <T extends ScanTask> boolean allTasksOfType(Class<T> javaClass) {
    return taskGroup.tasks().stream().allMatch(javaClass::isInstance);
  }

  public Table table() {
    return tableBroadcast.value();
  }

  public FileIO io() {
    return fileIOBroadcast.value();
  }

  public String branch() {
    return branch;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public boolean cacheDeleteFilesOnExecutors() {
    return cacheDeleteFilesOnExecutors;
  }

  public Schema expectedSchema() {
    if (expectedSchema == null) {
      this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
    }

    return expectedSchema;
  }
}
