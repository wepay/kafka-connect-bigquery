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


import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import io.netty.util.internal.ConcurrentSet;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * ThreadPoolExecutor for writing Rows to BigQuery.
 *
 * <p>Keeps track of the number of threads actively writing for each topic.
 * Keeps track of the number of failed threads in each batch of requests.
 */
public class KCBQThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger logger = LoggerFactory.getLogger(KCBQThreadPoolExecutor.class);

  private ConcurrentHashMap<String, AtomicInteger> activeThreadTopicCount =
      new ConcurrentHashMap<>();
  private ConcurrentSet<Throwable> encounteredErrors = new ConcurrentSet<>();
  private TopicPartitionManager topicPartitionManager;
  private int topicThreadLimit;

  /**
   * @param config the {@link BigQuerySinkTaskConfig}
   * @param context the {@link SinkTaskContext}
   * @param workQueue the queue for storing tasks.
   */
  public KCBQThreadPoolExecutor(BigQuerySinkTaskConfig config,
                                SinkTaskContext context,
                                BlockingQueue<Runnable> workQueue) {
    super(config.getInt(BigQuerySinkTaskConfig.THREAD_POOL_SIZE_CONFIG),
          config.getInt(BigQuerySinkTaskConfig.THREAD_POOL_SIZE_CONFIG),
          // the following line is irrelevant because the core and max thread counts are the same.
          1, TimeUnit.SECONDS,
          workQueue);
    topicPartitionManager = new TopicPartitionManager(context);
    topicThreadLimit = config.getInt(BigQuerySinkTaskConfig.TOPIC_MAX_THREADS_CONFIG);
  }

  @Override
  protected void beforeExecute(Thread thread, Runnable runnable) {
    super.beforeExecute(thread, runnable);

    if (runnable instanceof TableWriter) {
      TableWriter tableWriter = (TableWriter) runnable;
      String topic = tableWriter.getTopic();
      activeThreadTopicCount.putIfAbsent(topic, new AtomicInteger(0));

      int topicThreadCount = activeThreadTopicCount.get(topic).incrementAndGet();
      if (topicThreadCount > topicThreadLimit) {
        topicPartitionManager.pause(topic, topicThreadCount);
      }
    }
  }

  @Override
  protected void afterExecute(Runnable runnable, Throwable throwable) {
    super.afterExecute(runnable, throwable);

    if (runnable instanceof TableWriter) {
      TableWriter tableWriter = (TableWriter) runnable;
      String topic = tableWriter.getTopic();

      int topicThreadCount = activeThreadTopicCount.get(topic).decrementAndGet();
      if (topicThreadCount < topicThreadLimit) {
        topicPartitionManager.resume(topic);
      }
    }

    if (throwable != null) {
      logger.error("Task failed with {} error: {}",
                   throwable.getClass().getName(),
                   throwable.getMessage());
      encounteredErrors.add(throwable);
    }
  }

  /**
   * Wait for all the currently queued tasks to complete, and then return.
   *
   * @throws BigQueryConnectException if any of the tasks failed.
   * @throws InterruptedException if interrupted while waiting.
   */
  public void awaitCurrentTasks() throws InterruptedException, BigQueryConnectException {
    int maximumPoolSize = getMaximumPoolSize();
    CountDownLatch countDownLatch = new CountDownLatch(maximumPoolSize);
    for (int i = 0; i < maximumPoolSize; i++) {
      execute(new CountDownRunnable(countDownLatch));
    }
    countDownLatch.await();
    if (encounteredErrors.size() > 0) {
      String errorString = createErrorString(encounteredErrors);
      encounteredErrors.clear();
      throw new BigQueryConnectException("Some write threads encountered unrecoverable errors: "
                                         + errorString + "; See logs for more detail");
    }
  }

  private static String createErrorString(Collection<Throwable> errors) {
    List<String> exceptionTypeStrings = new ArrayList<>(errors.size());
    exceptionTypeStrings.addAll(errors.stream()
                        .map(throwable -> throwable.getClass().getName())
                        .collect(Collectors.toList()));
    return String.join(", ", exceptionTypeStrings);
  }

  private enum State {
    PAUSED,
    RUNNING
  }

  private static class TopicPartitionManager {

    private final SinkTaskContext context;
    private Map<TopicPartition, State> topicStates;
    private Map<TopicPartition, Long> topicChangeMs;

    public TopicPartitionManager(SinkTaskContext context) {
      this.context = context;
      topicStates = new HashMap<>();
      topicChangeMs = new HashMap<>();
    }

    public void pause(String topic, int threadCount) {
      Long now = System.currentTimeMillis();
      Collection<TopicPartition> topicPartitions = getPartitionsForTopic(topic);
      long oldestChangeMs = now;
      for (TopicPartition topicPartition : topicPartitions) {
        if (topicChangeMs.containsKey(topicPartition)) {
          oldestChangeMs = Math.min(oldestChangeMs, topicChangeMs.get(topicPartition));
        }
        topicStates.put(topicPartition, State.PAUSED);
        topicChangeMs.put(topicPartition, now);
        synchronized (context) {
          context.pause(topicPartition);
        }

      }

      logger.info("Paused all partitions for topic {} with thread count {} after {}ms: [{}]",
                  topic,
                  threadCount,
                  now - oldestChangeMs,
                  topicPartitionsString(topicPartitions));
    }

    public void resume(String topic) {
      Long now = System.currentTimeMillis();
      Collection<TopicPartition> topicPartitions = getPartitionsForTopic(topic);
      for (TopicPartition topicPartition : topicPartitions) {
        if (topicStates.containsKey(topicPartition)) {
          if (topicStates.get(topicPartition) == State.PAUSED) {
            // todo different logging info?
            logger.info("Restarting topicPartition {} from pause after {}ms",
                        topicPartition,
                        now - topicChangeMs.get(topicPartition));
            topicChangeMs.put(topicPartition, now);
          } else {
            logger.debug("'Restarting' already running partition {}",
                         topicPartition);
          }
        } else {
          logger.info("Restarting new topicPartition {}",
                      topicPartition);
          topicChangeMs.put(topicPartition, now);
        }
        topicStates.put(topicPartition, State.RUNNING);
        synchronized (context) {
          context.resume(topicPartition);
        }
      }
    }

    private Collection<TopicPartition> getPartitionsForTopic(String topic) {
      synchronized (context) {
        return context.assignment()
          .stream()
          .filter(topicPartition -> topicPartition.topic().equals(topic))
          .collect(Collectors.toList());
      }
    }

    private String topicPartitionsString(Collection<TopicPartition> topicPartitions) {
      List<String> topicPartitionStrings = topicPartitions.stream()
                                                          .map(TopicPartition::toString)
                                                          .collect(Collectors.toList());
      return String.join(", ", topicPartitionStrings);
    }
  }
}
