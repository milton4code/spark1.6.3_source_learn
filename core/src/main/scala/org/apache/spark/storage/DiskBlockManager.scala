/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http:// www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.util.UUID
import java.io.{IOException, File}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. By default, one block is mapped to one file with a name given by its BlockId.
 * However, it is also possible to have a block map to only a segment of a file, by calling
 * mapBlockToFileSegment().
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
// 创建和维护逻辑blocks和物理blocks在磁盘中的映射关系。
// 默认情况下，一个文件block块对应一个文件，文件名为Blockid。
//  当一个block对应的是一个文件片段时，调用mapBlockToFileSegment（）来维护blockh和文件片段关系
// block文件在spark通过hash目录，然后存放在list中。
private[spark] class DiskBlockManager(blockManager: BlockManager, conf: SparkConf)
  extends Logging {

  private[spark]
  val subDirsPerLocalDir = blockManager.conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  // 通过spark.local.dir 配置创建本地目录，通过对文件hash ，分配到多个子目录中，这样避免节点上顶级目录过于庞大
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  //  The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  //  of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories. */
  //  This method should be kept in sync with
  //  org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  def getFile(filename: String): File = {
    //  Figure out which local directory it hashes to, and which subdirectory in that
    // 根据文件名取 非负数hash
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    //  Create the subdirectory if it doesn't already exist
    // synchronized 防止多线程修改时造成数据不一致。
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        // 将dirId，subDirId组成二维数组
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    //  Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        //  Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  //  中间临时文件名的组成为 "temp_shuffle_" + UUID,组成一个唯一的blockId
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {

      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  //  为存储block数据创建本地目录。 这些目录是位于配置的本地目录内，不会再使用外shuffle服务及时jvm退出，件也不被删除。
  private def createLocalDirs(conf: SparkConf): Array[File] =
  //  在SparkConf中找出配置的文件保存路径，spark.local.dir，路径可以为多个，用逗号隔开。
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  private def addShutdownHook(): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    //  Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    //  Only perform cleanup if an external service is not serving our shuffle files.
    //  Also blockManagerId could be null if block manager is not initialized properly.
    if (!blockManager.externalShuffleServiceEnabled ||
      (blockManager.blockManagerId != null && blockManager.blockManagerId.isDriver)) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
