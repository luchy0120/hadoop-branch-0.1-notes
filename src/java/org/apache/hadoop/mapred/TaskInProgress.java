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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import java.io.*;
import java.util.*;
import java.util.logging.*;


////////////////////////////////////////////////////////
// TaskInProgress maintains all the info needed for a
// Task in the lifetime of its owning Job.  A given Task
// might be speculatively executed or reexecuted, so we
// need a level of indirection above the running-id itself.
//
// A given TaskInProgress contains multiple taskids,
// 0 or more of which might be executing at any one time.
// (That's what allows speculative execution.)  A taskid
// is now *never* recycled.  A TIP allocates enough taskids
// to account for all the speculation and failures it will
// ever have to handle.  Once those are up, the TIP is dead.
//
////////////////////////////////////////////////////////
class TaskInProgress {
    static final int MAX_TASK_EXECS = 1;
    static final int MAX_TASK_FAILURES = 4;    
    static final double SPECULATIVE_GAP = 0.2;
    static final long SPECULATIVE_LAG = 60 * 1000;

    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.mapred.TaskInProgress");

    // Defines the TIP
    private String jobFile = null;
    // task 需要处理的分片
    private FileSplit split = null;
    private String hints[][] = null;
    // 该task 之前需要处理的前置task们， 对reduce task 而言
    private TaskInProgress predecessors[] = null;
    // 所处理的partition
    private int partition;
    // jobtracker 是谁
    private JobTracker jobtracker;

    private String id;


    // 所有taskId ，包括备份的task
    private String totalTaskIds[];

    private JobInProgress job;

    // Status of the TIP
    private int numTaskFailures = 0;
    private double progress = 0;
    private String state = "";
    private long startTime = 0;
    private int completes = 0;
    private boolean failed = false;
    // 可用的taskid

    private TreeSet usableTaskIds = new TreeSet();
    private TreeSet recentTasks = new TreeSet();
    private JobConf conf;
    
    private TreeMap taskDiagnosticData = new TreeMap();
    private TreeMap taskStatuses = new TreeMap();

    private TreeSet machinesWhereFailed = new TreeSet();
    private TreeSet tasksReportedClosed = new TreeSet();

    /**
     * Constructor for MapTask
     */
    public TaskInProgress(String jobFile, FileSplit split, JobTracker jobtracker, JobConf conf, JobInProgress job) {
        this.jobFile = jobFile;
        // 分片
        this.split = split;
        // 所在的job
        this.jobtracker = jobtracker;
        this.job = job;
        // job的配置
        this.conf = conf;
        init();
    }
        
    /**
     * Constructor for ReduceTask
     */
    public TaskInProgress(String jobFile, TaskInProgress predecessors[], int partition, JobTracker jobtracker, JobConf conf, JobInProgress job) {
        this.jobFile = jobFile;
        // 前置的task们，前置的都是map task
        this.predecessors = predecessors;
        // 所在分片
        this.partition = partition;
        this.jobtracker = jobtracker;
        this.job = job;
        this.conf = conf;
        init();
    }

    /**
     * Initialization common to Map and Reduce
     */
    void init() {
        // 开始时间
        this.startTime = System.currentTimeMillis();
        // 给他个唯一id
        this.id = "tip_" + jobtracker.createUniqueId();
        // 所有taskid, 生成5个
        this.totalTaskIds = new String[MAX_TASK_EXECS + MAX_TASK_FAILURES];
        for (int i = 0; i < totalTaskIds.length; i++) {
            if (isMapTask()) {
                totalTaskIds[i] = "task_m_" + jobtracker.createUniqueId();
            } else {
                totalTaskIds[i] = "task_r_" + jobtracker.createUniqueId();
            }
            // 可用的taskid
            usableTaskIds.add(totalTaskIds[i]);
        }
    }

    ////////////////////////////////////
    // Accessors, info, profiles, etc.
    ////////////////////////////////////

    /**
     * Return the parent job
     */
    public JobInProgress getJob() {
        return job;
    }
    /**
     * Return an ID for this task, not its component taskid-threads
     */
    public String getTIPId() {
        return this.id;
    }
    /**
     * Whether this is a map task
     */
    // map 需要在分片上跑
    public boolean isMapTask() {
        return split != null;
    }
    /**
     */
    public boolean isComplete() {
        return (completes > 0);
    }
    /**
     */
    // 某个 taskid 是否完成
    public boolean isComplete(String taskid) {
        TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
        if (status == null) {
            return false;
        }
        return ((completes > 0) && (status.getRunState() == TaskStatus.SUCCEEDED));
    }
    /**
     */
    public boolean isFailed() {
        return failed;
    }

    /**
     * Number of times the TaskInProgress has failed.
     */
    // 失败的次数
    public int numTaskFailures() {
        return numTaskFailures;
    }

    /**
     * Get the overall progress (from 0 to 1.0) for this TIP
     */
    // 进度
    public double getProgress() {
        return progress;
    }
    /**
     * Returns whether a component task-thread should be 
     * closed because the containing JobInProgress has completed.
     */
    public boolean shouldCloseForClosedJob(String taskid) {
        // If the thing has never been closed,
        // and it belongs to this TIP,
        // and this TIP is somehow FINISHED,
        // then true
        TaskStatus ts = (TaskStatus) taskStatuses.get(taskid);
        if ((ts != null) &&
            (! tasksReportedClosed.contains(taskid)) &&
            (job.getStatus().getRunState() != JobStatus.RUNNING)) {
            // 在report close里面添加taskid
            tasksReportedClosed.add(taskid);
            return true;
        } else {
            return false;
        }
    }

    /**
     * A TaskInProgress might be speculatively executed, and so
     * can have many taskids simultaneously.  Reduce tasks rely on knowing
     * their predecessor ids, so they can be sure that all the previous
     * work has been completed.
     *
     * But we don't know ahead of time which task id will actually be
     * the one that completes for a given Map task.  We don't want the
     * Reduce task to have to be recreated after Map-completion, or check
     * in with the JobTracker.  So instead, each TaskInProgress preallocates
     * all the task-ids it could ever want to run simultaneously.  Then the
     * Reduce task can be told about all the ids task-ids for a given Map 
     * TaskInProgress.  If any of the Map TIP's tasks complete, the Reduce
     * task will know all is well, and can continue.
     *
     * Most of the time, only a small number of the possible task-ids will
     * ever be used.
     */
    // 可能可以备份地同时跑几个task， reduce只要检查到任意一个map task 完成了，就可以了
    public String[] getAllPossibleTaskIds() {
        return totalTaskIds;
    }

    /**
     * Creates a "status report" for this task.  Includes the
     * task ID and overall status, plus reports for all the
     * component task-threads that have ever been started.
     */
    TaskReport generateSingleReport() {
      ArrayList diagnostics = new ArrayList();
      for (Iterator i = taskDiagnosticData.values().iterator(); i.hasNext();) {
        diagnostics.addAll((Vector)i.next());
      }
      return new TaskReport
        (getTIPId(), (float)progress, state,
         (String[])diagnostics.toArray(new String[diagnostics.size()]));
    }

    ////////////////////////////////////////////////
    // Update methods, usually invoked by the owning
    // job.
    ////////////////////////////////////////////////
    /**
     * A status message from a client has arrived.
     * It updates the status of a single component-thread-task,
     * which might result in an overall TaskInProgress status update.
     */
    public void updateStatus(TaskStatus status) {
        String taskid = status.getTaskId();
        String diagInfo = status.getDiagnosticInfo();
        if (diagInfo != null && diagInfo.length() > 0) {
            LOG.info("Error from "+taskid+": "+diagInfo);
            Vector diagHistory = (Vector) taskDiagnosticData.get(taskid);
            if (diagHistory == null) {
                diagHistory = new Vector();
                taskDiagnosticData.put(taskid, diagHistory);
            }
            diagHistory.add(diagInfo);
        }
        taskStatuses.put(taskid, status);

        // Recompute progress
        recomputeProgress();
    }

    /**
     * Indicate that one of the taskids in this TaskInProgress
     * has failed.
     */
    public void failedSubTask(String taskid, String trackerName) {
        //
        // Note the failure and its location
        //
        LOG.info("Task '" + taskid + "' has been lost.");
        TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
        if (status != null) {
            status.setRunState(TaskStatus.FAILED);
        }
        this.recentTasks.remove(taskid);
        if (this.completes > 0) {
            this.completes--;
        }

        numTaskFailures++;
        if (numTaskFailures >= MAX_TASK_FAILURES) {
            LOG.info("TaskInProgress " + getTIPId() + " has failed " + numTaskFailures + " times.");
            kill();
        }
        machinesWhereFailed.add(trackerName);

        // Ask JobTracker to forget about this task
        jobtracker.removeTaskEntry(taskid);

        recomputeProgress();
    }

    /**
     * Indicate that one of the taskids in this TaskInProgress
     * has successfully completed!
     */
    public void completed(String taskid) {
        LOG.info("Task '" + taskid + "' has completed.");
        TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
        status.setRunState(TaskStatus.SUCCEEDED);
        recentTasks.remove(taskid);

        //
        // Now that the TIP is complete, the other speculative 
        // subtasks will be closed when the owning tasktracker 
        // reports in and calls shouldClose() on this object.
        //

        this.completes++;
        recomputeProgress();
    }

    /**
     * The TIP's been ordered kill()ed.
     */
    public void kill() {
        if (isComplete() || failed) {
            return;
        }
        this.failed = true;
        recomputeProgress();
    }

    /**
     * This method is called whenever there's a status change
     * for one of the TIP's sub-tasks.  It recomputes the overall 
     * progress for the TIP.  We examine all sub-tasks and find 
     * the one that's most advanced (and non-failed).
     */
    // 更新一下progress
    void recomputeProgress() {
        // 完成了，就是1
        if (isComplete()) {
            this.progress = 1;
        } else if (failed) {
            // 失败了是0
            this.progress = 0;
        } else {
            // 最好的进度
            double bestProgress = 0;
            String bestState = "";
            for (Iterator it = taskStatuses.keySet().iterator(); it.hasNext(); ) {
                // 某个tassk_id
                String taskid = (String) it.next();
                TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
                if (status.getRunState() == TaskStatus.SUCCEEDED) {
                    // 有成功的，最好的就是1
                    bestProgress = 1;
                    bestState = status.getStateString();
                    break;
                } else if (status.getRunState() == TaskStatus.RUNNING) {
                  if (status.getProgress() >= bestProgress) {
                      // 在跑，并且进度最超前
                    bestProgress = status.getProgress();
                    bestState = status.getStateString();
                  }
                }
            }
            this.progress = bestProgress;
            this.state = bestState;
        }
    }

    /////////////////////////////////////////////////
    // "Action" methods that actually require the TIP
    // to do something.
    /////////////////////////////////////////////////
    /**
     * Return whether this TIP has an DFS cache-driven task 
     * to run at the given taskTracker.
     */
    // 分片是否有一部分Block在这个tasktracker 上
    boolean hasTaskWithCacheHit(String taskTracker, TaskTrackerStatus tts) {
        if (failed || isComplete() || recentTasks.size() > 0) {
            return false;
        } else {
            try {
                if (isMapTask()) {
                    if (hints == null) {
                        // 本 map task 所需的 split  在哪些机器上有分布，一个split 可能分布在多台机器
                        hints = job.getFileCacheHints(getTIPId(), split.getFile(), split.getStart(), split.getLength());
                    }
                    if (hints != null) {
                        // 第一维 Block
                        for (int i = 0; i < hints.length; i++) {
                            // 第二维 host
                            for (int j = 0; j < hints[i].length; j++) {
                                // 这些hosts 名单里包含有 taskTracker
                                if (hints[i][j].equals(tts.getHost())) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            } catch (IOException ie) {
            }
            return false;
        }
    }
    /**
     * Return whether this TIP has a non-speculative task to run
     */
    boolean hasTask() {
        // 有一个标准的需要去run的task吗？
        if (failed || isComplete() || recentTasks.size() > 0) {
            return false;
        } else {
            for (Iterator it = taskStatuses.values().iterator(); it.hasNext(); ) {
                TaskStatus ts = (TaskStatus) it.next();
                // 如果已经有备份任务在run 了，那么就不属于一个标准任务
                if (ts.getRunState() == TaskStatus.RUNNING) {
                    return false;
                }
            }
            return true;
        }
    }
    /**
     * Return whether the TIP has a speculative task to run.  We
     * only launch a speculative task if the current TIP is really
     * far behind, and has been behind for a non-trivial amount of 
     * time.
     */
    // 有备份的task
    boolean hasSpeculativeTask(double averageProgress) {
        //
        // REMIND - mjc - these constants should be examined
        // in more depth eventually...
        // 是 map task 并且 开启了备份功能  并且最近的task 比较少 并且 当前的进度 比 平均的进度少
        // 并且 task 已经启动了挺久了
        // 也就是说启动了挺久 但是进度比较慢
        if (isMapTask() &&
            recentTasks.size() <= MAX_TASK_EXECS &&
            conf.getSpeculativeExecution() &&
            (averageProgress - progress >= SPECULATIVE_GAP) &&
            (System.currentTimeMillis() - startTime >= SPECULATIVE_LAG)) {
            return true;
        }
        return false;
    }
    
    /**
     * Return a Task that can be sent to a TaskTracker for execution.
     */
    // 从taskInprogress 生产出一个task
    public Task getTaskToRun(String taskTracker, TaskTrackerStatus tts, double avgProgress) {
        Task t = null;
        if (hasTaskWithCacheHit(taskTracker, tts) ||
            hasTask() || 
            hasSpeculativeTask(avgProgress)) {

            // 拿出第一个taskId
            String taskid = (String) usableTaskIds.first();
            usableTaskIds.remove(taskid);

            if (isMapTask()) {
                t = new MapTask(jobFile, taskid, split);
            } else {
                // 如果是一个reduce 的 task，获得所有前置 task
                String mapIdPredecessors[][] = new String[predecessors.length][];
                for (int i = 0; i < mapIdPredecessors.length; i++) {
                    // 获得 某个map task 的所有 taskId， 包括备份 的 task的id
                    mapIdPredecessors[i] = predecessors[i].getAllPossibleTaskIds();
                }
                t = new ReduceTask(jobFile, taskid, mapIdPredecessors, partition);
            }
            t.setConf(conf);

            // 近期的task
            recentTasks.add(taskid);

            // Ask JobTracker to note that the task exists
            jobtracker.createTaskEntry(taskid, taskTracker, this);
        }
        return t;
    }
}
