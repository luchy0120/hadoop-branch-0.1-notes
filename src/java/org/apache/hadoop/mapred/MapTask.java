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

package org.apache.hadoop.mapred;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/** A Map task. */
class MapTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (MapTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new MapTask(); }
       });
  }

  private FileSplit split;
  private MapOutputFile mapOutputFile;
  private Configuration conf;

  public MapTask() {}

  public MapTask(String jobFile, String taskId, FileSplit split) {
    super(jobFile, taskId);
    this.split = split;
  }

  public boolean isMapTask() {
      return true;
  }

  public TaskRunner createRunner(TaskTracker tracker) {
    return new MapTaskRunner(this, tracker, this.conf);
  }

  public FileSplit getSplit() { return split; }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    split.write(out);
    
  }
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    split = new FileSplit();
    split.readFields(in);
  }

  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {

    // open output files
    //  有几个reduce task就有分几个partition
    final int partitions = job.getNumReduceTasks();
    final SequenceFile.Writer[] outs = new SequenceFile.Writer[partitions];
    try {
      for (int i = 0; i < partitions; i++) {
        // 如果有多个partition，那么文件名为 part-1.out, part-2.out，每个partition都是一个sequenceFile
        // 他们都放在taskid的文件夹下
        // map出来的结果写到本地文件系统里
        outs[i] =
          new SequenceFile.Writer(FileSystem.getNamed("local", job),
                                  this.mapOutputFile.getOutputFile(getTaskId(), i).toString(),
                                  // key class
                                  job.getOutputKeyClass(),
                                  // value class
                                  job.getOutputValueClass());
      }
      // 根据key找到相应的partition
      final Partitioner partitioner =
        (Partitioner)job.newInstance(job.getPartitionerClass());

      OutputCollector partCollector = new OutputCollector() { // make collector
          public synchronized void collect(WritableComparable key,
                                           Writable value)
            throws IOException {
            // 往那个partition的SequenceFile写一个新的key，value对
            outs[partitioner.getPartition(key, value, partitions)]
              .append(key, value);
            reportProgress(umbilical);
          }
        };

      OutputCollector collector = partCollector;
      Reporter reporter = getReporter(umbilical, getProgress());

      boolean combining = job.getCombinerClass() != null;
      if (combining) {                            // add combining collector
        // 在partCollector 外部包装一层 combingCollector, 使用combiner而不是reducer去collect结果
        // combiner 会传入关于一个key的一些values，利用values去计算一个中间结果
        collector = new CombiningCollector(job, partCollector, reporter);
      }
      // 根据jobconf 获取 inputformat
      final RecordReader rawIn =                  // open input
        job.getInputFormat().getRecordReader
        (FileSystem.get(job), split, job, reporter);

      RecordReader in = new RecordReader() {      // wrap in progress reporter
          private float perByte = 1.0f /(float)split.getLength();

          public synchronized boolean next(Writable key, Writable value)
            throws IOException {
            // 读取了这个分片的百分之多少
            float progress =                        // compute progress
              (float)Math.min((rawIn.getPos()-split.getStart())*perByte, 1.0f);
            // 把这个读取进度报告一下
            reportProgress(umbilical, progress);

            // 读取一行记录，将key和value读到
            return rawIn.next(key, value);
          }
          public long getPos() throws IOException { return rawIn.getPos(); }
          public void close() throws IOException { rawIn.close(); }
        };

      MapRunnable runner =
        (MapRunnable)job.newInstance(job.getMapRunnerClass());

      try {
        runner.run(in, collector, reporter);      // run the map

        if (combining) {                          // flush combiner
          // 最后要flush一次，跑一下combiner
          ((CombiningCollector)collector).flush();
        }

      } finally {
        if (combining) { 
          ((CombiningCollector)collector).close(); 
        } 
        in.close();                               // close input
      }
    } finally {

      // 关闭所有Writer
      for (int i = 0; i < partitions; i++) {      // close output
        if (outs[i] != null) {
          outs[i].close();
        }
      }
    }
    done(umbilical);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.mapOutputFile = new MapOutputFile();
    this.mapOutputFile.setConf(conf);
  }

  public Configuration getConf() {
    return this.conf;
  }
  
}
