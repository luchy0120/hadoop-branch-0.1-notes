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

/** 
 * Protocol that a JobClient and the central JobTracker use to communicate.  The
 * JobClient can use these methods to submit a Job for execution, and learn about
 * the current system status.
 */ 
interface JobSubmissionProtocol {
    /**
     * Submit a Job for execution.  Returns the latest profile for
     * that job.
     */
    // 提交一个job
    public JobStatus submitJob(String jobFile) throws IOException;

    /**
     * Get the current status of the cluster
     * @return summary of the state of the cluster
     */
    // 获取集群状态
    public ClusterStatus getClusterStatus();
    
    /**
     * Kill the indicated job
     */
    // 杀死一个job
    public void killJob(String jobid);

    /**
     * Grab a handle to a job that is already known to the JobTracker
     */
    // 获取job的描述
    public JobProfile getJobProfile(String jobid);

    /**
     * Grab a handle to a job that is already known to the JobTracker
     */
    // 获得job的状态
    public JobStatus getJobStatus(String jobid);

    /**
     * Grab a bunch of info on the tasks that make up the job
     */
    // 根据jobid拿map task报告
    public TaskReport[] getMapTaskReports(String jobid);
    // 根据jobid拿reduce task报告
    public TaskReport[] getReduceTaskReports(String jobid);

    /**
     * A MapReduce system always operates on a single filesystem.  This 
     * function returns the fs name.  ('local' if the localfs; 'addr:port' 
     * if dfs).  The client can then copy files into the right locations 
     * prior to submitting the job.
     */
    // 返回底层的Filesystem 名字
    public String getFilesystemName() throws IOException;
}
