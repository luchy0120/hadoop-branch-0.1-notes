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

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile;

/** An {@link InputFormat} for {@link SequenceFile}s. */
public class SequenceFileInputFormat extends InputFormatBase {

  public SequenceFileInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }

  protected File[] listFiles(FileSystem fs, JobConf job)
    throws IOException {

    // 列出所有input 文件夹下的文件
    File[] files = super.listFiles(fs, job);

    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      if (file.isDirectory()) {                   // it's a MapFile
        files[i] = new File(file, MapFile.DATA_FILE_NAME); // use the data file
      }
    }
    return files;
  }

  public RecordReader getRecordReader(FileSystem fs, FileSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {
    // reporter就是一个可以报告给他状态的东西
    reporter.setStatus(split.toString());

    return new SequenceFileRecordReader(job, split);
  }

}

