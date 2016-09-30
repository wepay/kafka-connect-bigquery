package com.wepay.kafka.connect.bigquery.write.batch;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;

import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.junit.Assert;
import org.junit.Test;

import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.List;

public class DynamicBatchWriterTest {

  @Test
  @SuppressWarnings("unchecked")
  public void simpleTest() throws InterruptedException {
    // the starting size is exactly right.
    final int actualMaxSize = 500;

    BigQueryWriter mockWriter = mock(BigQueryWriter.class);
    doThrow(new BigQueryException(400, null)).when(mockWriter)
        .writeRows(anyObject(),
                   argThat(new ListIsAtLeast(actualMaxSize + 1)),
                   anyObject(),
                   anyObject());

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter);

    writeAll(dynamicBatchWriter, actualMaxSize);
    Assert.assertEquals(500, dynamicBatchWriter.getCurrentBatchSize());

    writeAll(dynamicBatchWriter, 600);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(300)),
                                           anyObject(),
                                           anyObject());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void increaseBatchSizeTest() throws InterruptedException {
    // the starting size is too small
    final int actualMaxSize = 1200;
    // the actual configured maxSize should end up being 1000,
    // even though we allow up to 1200 size batches here

    BigQueryWriter mockWriter = mock(BigQueryWriter.class);
    doThrow(new BigQueryException(400, null)).when(mockWriter)
        .writeRows(anyObject(),
                   argThat(new ListIsAtLeast(actualMaxSize + 1)),
                   anyObject(),
                   anyObject());

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter);

    writeAll(dynamicBatchWriter, 3500);
    // expected calls are:
    // 500 (success)
    // 1000 (success)
    // 2000 (failure)
    // 1000 (success)
    // 1000 (success)
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());

    writeAll(dynamicBatchWriter, 1600);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(800)),
                                           anyObject(),
                                           anyObject());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void decreaseBatchSizeTest() throws InterruptedException {
    // the starting size is too large
    final int actualMaxSize = 300;
    // the actual configured maxSize should end up being 250,
    // even though we allow up to 300 size batches here.

    BigQueryWriter mockWriter = mock(BigQueryWriter.class);
    doThrow(new BigQueryException(400, null)).when(mockWriter)
        .writeRows(anyObject(),
                   argThat(new ListIsAtLeast(actualMaxSize + 1)),
                   anyObject(),
                   anyObject());

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter);

    writeAll(dynamicBatchWriter, 750);
    // expected calls are:
    // 500 (failure)
    // 250 (success)
    // 500 (failure)
    // 250 (success)
    // 250 (success)
    Assert.assertEquals(250, dynamicBatchWriter.getCurrentBatchSize());

    writeAll(dynamicBatchWriter, 400);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(200)),
                                           anyObject(),
                                           anyObject());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void establishedFailure() throws InterruptedException {
    // test a failure during an establishedWriteAll
    BigQueryWriter mockWriter = mock(BigQueryWriter.class);
    // start by establishing at 500, no seeking.
    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter, 500, false);

    // but we error at anything above 300:
    doThrow(new BigQueryException(400, null)).when(mockWriter)
        .writeRows(anyObject(),
                   argThat(new ListIsAtLeast(301)),
                   anyObject(),
                   anyObject());

    writeAll(dynamicBatchWriter, 500);
    // expected calls are:
    // 500 (failure)
    // 250 (success)
    // 250 (success)
    verify(mockWriter, times(1)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(500)),
                                           anyObject(),
                                           anyObject());
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(250)),
                                           anyObject(),
                                           anyObject());
    Assert.assertEquals(250, dynamicBatchWriter.getCurrentBatchSize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void establishedSuccesses() throws InterruptedException {
    // test a failure during an establishedWriteAll
    BigQueryWriter mockWriter = mock(BigQueryWriter.class);
    // start by establishing at 500, no seeking.
    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter, 500, false);

    // we only error at above 1100:
    doThrow(new BigQueryException(400, null)).when(mockWriter)
        .writeRows(anyObject(),
                   argThat(new ListIsAtLeast(1101)),
                   anyObject(),
                   anyObject());

    // 10 calls before batchSize increase
    for (int i = 0; i < 10; i++) {
      writeAll(dynamicBatchWriter, 1200);
    }
    verify(mockWriter, times(30)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(400)),
                                           anyObject(),
                                           anyObject());
    // verify we got up to 100 batch size
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());

    // actually write at 1000 batch size
    writeAll(dynamicBatchWriter, 1200);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(600)),
                                           anyObject(),
                                           anyObject());
    // verify batch size hasn't changed
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());
  }

  // todo tests:
  //    verify uneven established batch sizing.
  //    max size batch sizing establishment

  /**
   * Call writeAll with the given number of "elements".
   * @param dynamicBatchWriter the {@link DynamicBatchWriter}to use.
   * @param numElements the number of "elements" to "write"
   */
  private void writeAll(DynamicBatchWriter dynamicBatchWriter, int numElements)
      throws InterruptedException {
    List<InsertAllRequest.RowToInsert> elements = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      elements.add(null);
    }
    dynamicBatchWriter.writeAll(null, elements, null, null);
  }

  private static class ListIsAtLeast extends ArgumentMatcher<List> {
    private int size;

    ListIsAtLeast(int size) {
      this.size = size;
    }

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof List) {
        List list = (List) argument;
        return list.size() >= this.size;
      }
      return false;
    }
  }

  private static class ListIsExactly extends ArgumentMatcher<List> {
    private int size;

    ListIsExactly(int size) {
      this.size = size;
    }

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof List) {
        List list = (List) argument;
        return list.size() == this.size;
      }
      return false;
    }
  }
}
