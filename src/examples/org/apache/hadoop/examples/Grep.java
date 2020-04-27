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
package org.apache.hadoop.examples;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.apache.hadoop.mapred.lib.RegexMapper;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Random;

/* Extracts matching regexs from input files and counts them. */
public class Grep {
  private Grep() {}                               // singleton

  // 输入文件，输出文件，mapper，reducer，combiner，输入文件格式，输出文件格式，key，value数据类型
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
      System.exit(-1);
    }

    Configuration defaults = new Configuration();
    // 随机一个grep-temp-1231244文件作为输出文件
    File tempDir =
      new File("grep-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf grepJob = new JobConf(defaults, Grep.class);
    grepJob.setJobName("grep-search");
    // 设置输入文件名
    grepJob.setInputDir(new File(args[0]));
    // 设置mapper的class
    grepJob.setMapperClass(RegexMapper.class);
    // 设置参数
    grepJob.set("mapred.mapper.regex", args[2]);
    if (args.length == 4)
      grepJob.set("mapred.mapper.regex.group", args[3]);
    
    grepJob.setCombinerClass(LongSumReducer.class);
    grepJob.setReducerClass(LongSumReducer.class);

    grepJob.setOutputDir(tempDir);
    grepJob.setOutputFormat(SequenceFileOutputFormat.class);
    grepJob.setOutputKeyClass(UTF8.class);
    grepJob.setOutputValueClass(LongWritable.class);

    JobClient.runJob(grepJob);

    JobConf sortJob = new JobConf(defaults, Grep.class);
    sortJob.setJobName("grep-sort");
    // 从上一次的输出文件中读入
    sortJob.setInputDir(tempDir);
    sortJob.setInputFormat(SequenceFileInputFormat.class);
    sortJob.setInputKeyClass(UTF8.class);
    sortJob.setInputValueClass(LongWritable.class);

    sortJob.setMapperClass(InverseMapper.class);

    sortJob.setNumReduceTasks(1);                 // write a single file
    // 设置输出结果文件夹
    sortJob.setOutputDir(new File(args[1]));
    sortJob.setOutputKeyComparatorClass           // sort by decreasing freq
      (LongWritable.DecreasingComparator.class);

    JobClient.runJob(sortJob);

    new JobClient(defaults).getFs().delete(tempDir);
  }

}
