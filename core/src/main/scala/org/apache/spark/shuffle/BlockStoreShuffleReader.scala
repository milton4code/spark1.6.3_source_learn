/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  // TODO 为reduce 任务读取合并的key-values值
  override def read(): Iterator[Product2[K, C]] = {
    // ShuffleBlockFetcherIterator 初始化会调用initialize
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      /*
         当shuffleManager完成后注册到mapOutputTracker的元数据信息同样会通过mapOutputTracker来获取
         指定获取的分区范围，
       */
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      /*
         限定读取大小，对应后面的并发读取 ，
         这是一种数据读取的控制策略，一方面避免机器占用过多带宽
       */
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024) // 默认48M

    // Wrap the streams for compression based on configuration
    // 针对之前读取的各个blockId信息及对应的输入数据进行处理
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      blockManager.wrapForCompression(blockId, inputStream)
    }

    val ser = Serializer.getSerializer(dep.serializer)
    val serializerInstance = ser.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    // 用来保存中断或者取消掉任务
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) { //  对读取的数据进行聚合处理
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        // 针对map端各分区对key进行合并的数据再次进行聚合
        // map的合并可以大大减少网络传输数据量
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // 针对未进行合并的数据进行处理
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    // 对数据进行排序操作
    /*
      基于sort的Shuffle实现过程中，默认仅仅时基于partitionId进行排序
      分区内部的数据是没有排序的，因此添加keyOrdering变量
      需要提供是否需要针对分区内数据进行排序默认标识
     */
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        // Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        /*
           为了减少内存压力，避免GC开销，引入外部排序器对数据进行排序
           当内存不足时根据 spark.shuffle.spill确定是否溢写到磁盘，默认打开
           若不打开，数据量较大时会引起OOM
         */
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}
