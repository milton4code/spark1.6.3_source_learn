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

import org.apache.spark.{TaskContext, ShuffleDependency}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 */
private[spark] trait ShuffleManager {
  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  /*
    每个RDD的父依赖时，都会先注册到ShuffleManager,获取到shuffleManger后才能进行读写操作
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  /*
      TODO 通过ShuffleHandle获取block写入器，写入数据时通过ShuffleBlockResolver 获取文件写入位置，
      然后写入到相应位置，磁盘，内存等地方
    */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  // 通过ShuffleHandle 获取block 读取器，然后通过ShuffleBlockResolver获取指定block数据
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
    * Remove a shuffle's metadata from the ShuffleManager.
    * @return true if the metadata removed successfully, otherwise false.
    */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  // TODO block解析器
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
