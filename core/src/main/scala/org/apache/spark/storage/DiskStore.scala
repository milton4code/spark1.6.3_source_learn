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

package org.apache.spark.storage

import java.io.{IOException, File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * Stores BlockManager blocks on disk.
 */
private[spark] class DiskStore(blockManager: BlockManager, diskManager: DiskBlockManager)
  extends BlockStore(blockManager) with Logging {

  val minMemoryMapBytes = blockManager.conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  override def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()
    logDebug(s"Attempting to put block $blockId")
    /* 記錄開始時間 */
    val startTime = System.currentTimeMillis

    /*
     获取hash映射得到对应文件。
     blockId 时通过CacheManager.getOrCompute() 方法获取到，具体查看源码，将Rdd 的id 和Rdd的分区索引得到blockId,
     将rdd中partition转化为Storage模块中block.
     */
    val file = diskManager.getFile(blockId)
    //获取输出流 管道对象
    val channel = new FileOutputStream(file).getChannel
    Utils.tryWithSafeFinally {
      while (bytes.remaining > 0) {
        channel.write(bytes)
      }
    } {
      channel.close()
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(bytes.limit), finishTime - startTime))
     /*
      将block添加到BlockStore中。主要做了三件事
      1、输入数据的估计大小
      2、返回放入的数据
      3、因此次Put数据而不得不dorop掉的block列表，DiskStore中始终为空。
   */
    PutResult(bytes.limit(), Right(bytes.duplicate()))
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values.toIterator, level, returnValues)
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {

    logDebug(s"Attempting to write values for block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val outputStream = new FileOutputStream(file)
    try {
      Utils.tryWithSafeFinally {
        blockManager.dataSerializeStream(blockId, outputStream, values)
      } {
        // Close outputStream here because it should be closed before file is deleted.
        outputStream.close()
      }
    } catch {
      case e: Throwable =>
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
        throw e
    }

    val length = file.length

    val timeTaken = System.currentTimeMillis - startTime
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(length), timeTaken))

    if (returnValues) {
      // Return a byte buffer for the contents of the file
      val buffer = getBytes(blockId).get
      PutResult(length, Right(buffer))
    } else {
      PutResult(length, null)
    }
  }

  private def getBytes(file: File, offset: Long, length: Long): Option[ByteBuffer] = {
    //以只读的方式获取file 通道
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
      // For small files, directly read rather than memory map
      //小于2M 的文件直接读取，大于2M则使用内存映射方式读取，这样做，可以提高性能
      if (length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(length.toInt)
        channel.position(offset)
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) {
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=$offset\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
          }
        }
        buf.flip()
        Some(buf)
      } else {
        //通过channel map方法，使用内存映射方式读取文件
        Some(channel.map(MapMode.READ_ONLY, offset, length))
      }
    } {
      channel.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] =
  //计算blockId中name属性的hash值匹配的文件
    val file = diskManager.getFile(blockId.name)
    getBytes(file, 0, file.length)
  }

  def getBytes(segment: FileSegment): Option[ByteBuffer] = {
    getBytes(segment.file, segment.offset, segment.length)
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  override def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}
