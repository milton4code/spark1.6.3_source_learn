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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }
 //  逻辑地址：Pagenumber由13个bit组成，51bit组成Offset
  //TODO 申请内存空间
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    if (shouldPool(size)) {
      synchronized (this) {
        // 使用WeakReference  GC 时会优先回收该对象
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) {
              assert (memory.size() == size);
              return memory;
            }
          }
          bufferPoolsBySize.remove(size);
        }
      }
    }

    // 内存对齐，array里面都是地址，因为GC的时候对象的地址会发生变化，因此就需要 为了获得对象的引用。也就是对象的地址。
    long[] array = new long[(int) ((size + 7) / 8)];
      //array里面保存的都是地址，而LONG_ARRAY_OFFSET是偏移量，因此二者就可以定位到绝对地址，然后根据size就可以确定数据。
      return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
  }

  @Override
  public void free(MemoryBlock memory) {
    final long size = memory.size();
    if (shouldPool(size)) {
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(size, pool);
        }
        pool.add(new WeakReference<>(memory));
      }
    } else {
      // Do nothing
    }
  }
}
