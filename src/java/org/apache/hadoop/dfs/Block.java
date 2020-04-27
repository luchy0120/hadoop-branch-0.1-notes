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
package org.apache.hadoop.dfs;

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;
// block文件的名称：   blk_2312438942389
/**************************************************
 * A Block is a Hadoop FS primitive, identified by a 
 * long.
 *
 * @author Mike Cafarella
 **************************************************/
class Block implements Writable, Comparable {

    static {                                      // register a ctor
      WritableFactories.setFactory
        (Block.class,
         new WritableFactory() {
           public Writable newInstance() { return new Block(); }
         });
    }

    static Random r = new Random();

    /**
     */
    public static boolean isBlockFilename(File f) {
        if (f.getName().startsWith("blk_")) {
            return true;
        } else {
            return false;
        }
    }

    long blkid;
    long len;       // 多少byte的数据

    /**
     */
    public Block() {
        this.blkid = r.nextLong();   // 随机一个id
        this.len = 0;
    }

    /**
     */
    public Block(long blkid, long len) {
        this.blkid = blkid;
        this.len = len;
    }

    /**
     * Find the blockid from the given filename
     */
    // 文件名去掉前面的blk_，得到id
    public Block(File f, long len) {
        String name = f.getName();
        name = name.substring("blk_".length());
        this.blkid = Long.parseLong(name);
        this.len = len;
    }

    /**
     */
    public long getBlockId() {
        return blkid;
    }

    /**
     */
    public String getBlockName() {
        return "blk_" + String.valueOf(blkid);
    }

    /**
     */
    public long getNumBytes() {
        return len;
    }
    public void setNumBytes(long len) {
        this.len = len;
    }

    /**
     */
    public String toString() {
        return getBlockName();
    }

    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        out.writeLong(blkid);
        out.writeLong(len);
    }

    public void readFields(DataInput in) throws IOException {
        this.blkid = in.readLong();
        this.len = in.readLong();
    }

    /////////////////////////////////////
    // Comparable
    /////////////////////////////////////
    // 比较id
    public int compareTo(Object o) {
        Block b = (Block) o;
        if (getBlockId() < b.getBlockId()) {
            return -1;
        } else if (getBlockId() == b.getBlockId()) {
            return 0;
        } else {
            return 1;
        }
    }
    public boolean equals(Object o) {
        Block b = (Block) o;
        return (this.compareTo(b) == 0);
    }
}
