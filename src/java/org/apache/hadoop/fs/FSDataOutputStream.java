/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.*;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;

/** Utility that wraps a {@link FSOutputStream} in a {@link DataOutputStream},
 * buffers output through a {@link BufferedOutputStream} and creates a checksum
 * file. */
public class FSDataOutputStream extends DataOutputStream {
  public static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};
  
  /** Store checksums for data. */
  private static class Summer extends FilterOutputStream {
    // 写到checksum的stream
    private FSDataOutputStream sums;
    private Checksum sum = new CRC32();
    private int inSum;
    private int bytesPerSum;

    // 根据文件名创建出对应checksum文件的stream
    // 先写checksum的版本，再写计算单个checksum需要多少bytes
    public Summer(FileSystem fs, File file, boolean overwrite, Configuration conf)
      throws IOException {
      super(fs.createRaw(file, overwrite));
      this.bytesPerSum = conf.getInt("io.bytes.per.checksum", 512);
      this.sums =
        new FSDataOutputStream(fs.createRaw(fs.getChecksumFile(file), true), conf);

      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(this.bytesPerSum);
    }
    // 先算checksum，但未必满512个byte，所以未必每次写都会写出checksum，
    // 再调用底层的write，将数据写出到文件
    public void write(byte b[], int off, int len) throws IOException {
      int summed = 0;  // 一个指针，从off走到off+len位置
      while (summed < len) {

        int goal = this.bytesPerSum - inSum;  // 还需要写出多少
        int inBuf = len - summed;  // b数组里未写出多少bytes
        int toSum = inBuf <= goal ? inBuf : goal;  // 两者取较小值

        sum.update(b, off+summed, toSum);  // 使用toSum个byte计算一下
        summed += toSum;  // 使用了，往后移动

        inSum += toSum; // 统计一下，使用了多少
        if (inSum == this.bytesPerSum) { // 写完了
          writeSum();
        }
      }

      out.write(b, off, len);
    }
    // 写出一个checksum
    private void writeSum() throws IOException {
      if (inSum != 0) {
        sums.writeInt((int)sum.getValue());    // 将计算出的checksum写出
        sum.reset();
        inSum = 0;
      }
    }

    public void close() throws IOException {
      writeSum();    // 如果突然关掉了，可能不满512个byte，那么将此时的checksum写出
      sums.close();
      super.close();
    }

  }
  // 记录下当前的position
  private static class PositionCache extends FilterOutputStream {
    long position;

    public PositionCache(OutputStream out) throws IOException {
      super(out);
    }

    // This is the only write() method called by BufferedOutputStream, so we
    // trap calls to it in order to cache the position.
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
  }

  private static class Buffer extends BufferedOutputStream {
    public Buffer(OutputStream out, int bufferSize) throws IOException {
      super(out, bufferSize);
    }
    // 获得底层的positionCache里的position，再加上自己的count个数，作为本层的position
    public long getPos() throws IOException {
      return ((PositionCache)out).getPos() + this.count;
    }
    // 每次都先写入buf数组，如果写不下了，那么调用BufferedOutputSteam的写出方法
    // optimized version of write(int)
    public void write(int b) throws IOException {
      if (count >= buf.length) {
        super.write(b);
      } else {
        buf[count++] = (byte)b;
      }
    }

  }

  public FSDataOutputStream(FileSystem fs, File file,
                            boolean overwrite, Configuration conf,
                            int bufferSize)
    throws IOException {
    super(new Buffer(new PositionCache(new Summer(fs, file, overwrite, conf)),
                     bufferSize));
  }
  // 写出文件时指定的buffer数组的size
  /** Construct without checksums. */
  private FSDataOutputStream(FSOutputStream out, Configuration conf) throws IOException {
    this(out, conf.getInt("io.file.buffer.size", 4096));
  }

  /** Construct without checksums. */
  private FSDataOutputStream(FSOutputStream out, int bufferSize)
    throws IOException {
    super(new Buffer(new PositionCache(out), bufferSize));
  }

  public long getPos() throws IOException {
    return ((Buffer)out).getPos();
  }

}
