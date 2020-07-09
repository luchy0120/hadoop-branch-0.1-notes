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
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 * @author Mike Cafarella
 *******************************************************/
public class JobTracker implements MRConstants, InterTrackerProtocol, JobSubmissionProtocol {
    static long JOBINIT_SLEEP_INTERVAL = 2000;
    static long RETIRE_JOB_INTERVAL;
    static long RETIRE_JOB_CHECK_INTERVAL;
    static float TASK_ALLOC_EPSILON;
    static float PAD_FRACTION;
    static float MIN_SLOTS_FOR_PADDING;

    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.mapred.JobTracker");

    private static JobTracker tracker = null;
    public static void startTracker(Configuration conf) throws IOException {
      if (tracker != null)
        throw new IOException("JobTracker already running.");
      while (true) {
        try {
          tracker = new JobTracker(conf);
          break;
        } catch (IOException e) {
          LOG.log(Level.WARNING, "Starting tracker", e);
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
      tracker.offerService();
    }

    public static JobTracker getTracker() {
        return tracker;
    }

    ///////////////////////////////////////////////////////
    // Used to expire TaskTrackers that have gone down
    ///////////////////////////////////////////////////////
    class ExpireTrackers implements Runnable {
        boolean shouldRun = true;
        public ExpireTrackers() {
        }
        /**
         * The run method lives for the life of the JobTracker, and removes TaskTrackers
         * that have not checked in for some time.
         */
        // 将很久未联系的taskTracker 移除
        public void run() {
            while (shouldRun) {
                //
                // Thread runs periodically to check whether trackers should be expired.
                // The sleep interval must be no more than half the maximum expiry time
                // for a task tracker.
                //
                try {
                    Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);
                } catch (InterruptedException ie) {
                }

                //
                // Loop through all expired items in the queue
                //
                synchronized (taskTrackers) {
                    synchronized (trackerExpiryQueue) {
                        long now = System.currentTimeMillis();
                        // 获取当前时间
                        TaskTrackerStatus leastRecent = null;
                        // 最近最少访问的TaskTracker，并且最后访问的时间过去很久
                        while ((trackerExpiryQueue.size() > 0) &&
                               ((leastRecent = (TaskTrackerStatus) trackerExpiryQueue.first()) != null) &&
                               (now - leastRecent.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL)) {

                            // Remove profile from head of queue
                            trackerExpiryQueue.remove(leastRecent);
                            // 最少访问的tracker的名字
                            String trackerName = leastRecent.getTrackerName();

                            // Figure out if last-seen time should be updated, or if tracker is dead
                            TaskTrackerStatus newProfile = (TaskTrackerStatus) taskTrackers.get(leastRecent.getTrackerName());
                            // Items might leave the taskTracker set through other means; the
                            // status stored in 'taskTrackers' might be null, which means the
                            // tracker has already been destroyed.
                            if (newProfile != null) {
                                if (now - newProfile.getLastSeen() > TASKTRACKER_EXPIRY_INTERVAL) {
                                    // Remove completely
                                    updateTaskTrackerStatus(trackerName, null);
                                    lostTaskTracker(leastRecent.getTrackerName());
                                } else {
                                    // Update time by inserting latest profile
                                    trackerExpiryQueue.add(newProfile);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        /**
         * Stop the tracker on next iteration
         */
        public void stopTracker() {
            shouldRun = false;
        }
    }

    ///////////////////////////////////////////////////////
    // Used to remove old finished Jobs that have been around for too long
    ///////////////////////////////////////////////////////
    class RetireJobs implements Runnable {
        boolean shouldRun = true;
        public RetireJobs() {
        }

        /**
         * The run method lives for the life of the JobTracker,
         * and removes Jobs that are not still running, but which
         * finished a long time ago.
         */
        public void run() {
            while (shouldRun) {
                // 每隔一段时间
                try {
                    Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
                } catch (InterruptedException ie) {
                }
                
                synchronized (jobs) {
                    synchronized (jobInitQueue) {
                        synchronized (jobsByArrival) {
                            for (Iterator it = jobs.keySet().iterator(); it.hasNext(); ) {
                                String jobid = (String) it.next();
                                JobInProgress job = (JobInProgress) jobs.get(jobid);

                                // job 不是准备也不是runnning状态，job完成时间在很久之前
                                if (job.getStatus().getRunState() != JobStatus.RUNNING &&
                                    job.getStatus().getRunState() != JobStatus.PREP &&
                                    (job.getFinishTime() + RETIRE_JOB_INTERVAL < System.currentTimeMillis())) {
                                    // 删掉job
                                    it.remove();

                                    jobInitQueue.remove(job);
                                    jobsByArrival.remove(job);
                                }
                            }
                        }
                    }
                }
            }
        }
        public void stopRetirer() {
            shouldRun = false;
        }
    }

    /////////////////////////////////////////////////////////////////
    //  Used to init new jobs that have just been created
    /////////////////////////////////////////////////////////////////
    class JobInitThread implements Runnable {
        boolean shouldRun = true;
        public JobInitThread() {
        }
        public void run() {
            while (shouldRun) {
                JobInProgress job = null;
                synchronized (jobInitQueue) {
                    if (jobInitQueue.size() > 0) {
                        job = (JobInProgress) jobInitQueue.elementAt(0);
                        jobInitQueue.remove(job);
                    } else {
                        try {
                            // 等待，重试，查看queue 里面的job
                            jobInitQueue.wait(JOBINIT_SLEEP_INTERVAL);
                        } catch (InterruptedException iex) {
                        }
                    }
                }
                try {
                    if (job != null) {
                        // job初始化tasks
                        job.initTasks();
                    }
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "job init failed", e);
                    job.kill();
                }
            }
        }
        public void stopIniter() {
            shouldRun = false;
        }
    }


    /////////////////////////////////////////////////////////////////
    // The real JobTracker
    ////////////////////////////////////////////////////////////////
    // 端口
    int port;
    // 本地机器名
    String localMachine;
    // 开始时间
    long startTime;
    // 总共提交
    int totalSubmissions = 0;
    // 随机数
    Random r = new Random();
    //
    private int maxCurrentTasks;

    //
    // Properties to maintain while running Jobs and Tasks:
    //
    // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its 
    //     Tasks are complete.
    //
    // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
    //
    // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
    //
    // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
    //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
    //     reexecution if all downstream Tasks that require its output have already obtained
    //     the necessary files.
    //

    // All the known jobs.  (jobid->JobInProgress)

    // 一个job包含所有task

    TreeMap jobs = new TreeMap();
    // 根据先来后到排列的队列
    Vector jobsByArrival = new Vector();

    // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
    TreeMap taskidToTIPMap = new TreeMap();

    // (taskid --> trackerID)
    // 使用taskid 获得trackerid
    TreeMap taskidToTrackerMap = new TreeMap();

    // (trackerID->TreeSet of taskids running at that tracker)
    // 使用trackerid获得taskids
    TreeMap trackerToTaskMap = new TreeMap();

    //
    // Watch and expire TaskTracker objects using these structures.
    // We can map from Name->TaskTrackerStatus, or we can expire by time.
    //
    // 所有maps
    int totalMaps = 0;
    // 所有reduce
    int totalReduces = 0;

    // 所有的 tasktrackers
    private TreeMap taskTrackers = new TreeMap();
    // job初始化队列
    Vector jobInitQueue = new Vector();

    // 检测过期tracker
    ExpireTrackers expireTrackers = new ExpireTrackers();

    // 检测过期jobs
    RetireJobs retireJobs = new RetireJobs();
    // job初始化
    JobInitThread initJobs = new JobInitThread();

    /**
     * It might seem like a bug to maintain a TreeSet of status objects,
     * which can be updated at any time.  But that's not what happens!  We
     * only update status objects in the taskTrackers table.  Status objects
     * are never updated once they enter the expiry queue.  Instead, we wait
     * for them to expire and remove them from the expiry queue.  If a status
     * object has been updated in the taskTracker table, the latest status is 
     * reinserted.  Otherwise, we assume the tracker has expired.
     */
    TreeSet trackerExpiryQueue = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            TaskTrackerStatus p1 = (TaskTrackerStatus) o1;
            TaskTrackerStatus p2 = (TaskTrackerStatus) o2;
            // 根据最后看到的时间排序
            if (p1.getLastSeen() < p2.getLastSeen()) {
                return -1;
            } else if (p1.getLastSeen() > p2.getLastSeen()) {
                return 1;
            } else {
                return (p1.getTrackerName().compareTo(p2.getTrackerName()));
            }
        }
    });

    // Used to provide an HTML view on Job, Task, and TaskTracker structures
    JobTrackerInfoServer infoServer;
    // info web的端口
    int infoPort;

    Server interTrackerServer;

    // Some jobs are stored in a local system directory.  We can delete
    // the files when we're done with the job.
    static final String SUBDIR = "jobTracker";
    // 文件系统
    FileSystem fs;
    // 文件夹
    File systemDir;
    // 配置
    private Configuration conf;

    /**
     * Start the JobTracker process, listen on the indicated port
     */
    JobTracker(Configuration conf) throws IOException {
        //
        // Grab some static constants
        // 一个tracker 最多同时执行多少task
        maxCurrentTasks = conf.getInt("mapred.tasktracker.tasks.maximum", 2);
        // job 隔多久被retire
        RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
        // 多久会check一次 job是否被retire
        RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);

        TASK_ALLOC_EPSILON = conf.getFloat("mapred.jobtracker.taskalloc.loadbalance.epsilon", 0.2f);
        PAD_FRACTION = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 0.1f);

        // 最少slots
        MIN_SLOTS_FOR_PADDING = 3 * maxCurrentTasks;

        // This is a directory of temporary submission files.  We delete it
        // on startup, and can delete any files that we're done with
        this.conf = conf;
        JobConf jobConf = new JobConf(conf);
        // 一个临时文件夹 // tmp/mapred
        this.systemDir = jobConf.getSystemDir();
        // 获得hdfs
        this.fs = FileSystem.get(conf);
        // 在hdfs上删除sysdir
        FileUtil.fullyDelete(fs, systemDir);
        // 新建sysdir
        fs.mkdirs(systemDir);

        // Same with 'localDir' except it's always on the local disk.
        // 删除本地文件夹 jobtracker
        jobConf.deleteLocalFiles(SUBDIR);

        // Set ports, start RPC servers, etc.
        // jobtracker hostname 端口
        InetSocketAddress addr = getAddress(conf);

        // 本地机器名
        this.localMachine = addr.getHostName();
        // 端口
        this.port = addr.getPort();
        // 启动 server，传入this，作为被调用的instance，10个handler线程来处理
        this.interTrackerServer = RPC.getServer(this, addr.getPort(), 10, false, conf);
        //
        this.interTrackerServer.start();
        // 打印system properties
	Properties p = System.getProperties();
	for (Iterator it = p.keySet().iterator(); it.hasNext(); ) {
	    String key = (String) it.next();
	    String val = (String) p.getProperty(key);
	    LOG.info("Property '" + key + "' is " + val);
	}

	// 打开web页面
        this.infoPort = conf.getInt("mapred.job.tracker.info.port", 50030);
        this.infoServer = new JobTrackerInfoServer(this, infoPort);
        this.infoServer.start();
    // 开始时间
        this.startTime = System.currentTimeMillis();

        // 启动expiretracker
        new Thread(this.expireTrackers).start();
        // 启动retirejob
        new Thread(this.retireJobs).start();
        // 启动初始化jobs
        new Thread(this.initJobs).start();
    }

    public static InetSocketAddress getAddress(Configuration conf) {
      String jobTrackerStr =
        conf.get("mapred.job.tracker", "localhost:8012");
      int colon = jobTrackerStr.indexOf(":");
      if (colon < 0) {
        throw new RuntimeException("Bad mapred.job.tracker: "+jobTrackerStr);
      }
      // tracker的名字  localhost
      String jobTrackerName = jobTrackerStr.substring(0, colon);
      // tracker的端口
      int jobTrackerPort = Integer.parseInt(jobTrackerStr.substring(colon+1));
      //
      return new InetSocketAddress(jobTrackerName, jobTrackerPort);
    }


    /**
     * Run forever
     */
    // 一直run
    public void offerService() {
        try {
            this.interTrackerServer.join();
        } catch (InterruptedException ie) {
        }
    }

    ///////////////////////////////////////////////////////
    // Maintain lookup tables; called by JobInProgress
    // and TaskInProgress
    ///////////////////////////////////////////////////////
    // 为 tasktracker创建 一个taskinprogress
    void createTaskEntry(String taskid, String taskTracker, TaskInProgress tip) {
        LOG.info("Adding task '" + taskid + "' to tip " + tip.getTIPId() + ", for tracker '" + taskTracker + "'");

        // taskid --> tracker
        // 通过taskid 查询task tracker
        taskidToTrackerMap.put(taskid, taskTracker);

        // tracker --> taskid
        TreeSet taskset = (TreeSet) trackerToTaskMap.get(taskTracker);
        if (taskset == null) {
            taskset = new TreeSet();
            trackerToTaskMap.put(taskTracker, taskset);
        }
        taskset.add(taskid);

        // taskid --> TIP ， task id 和 task in progress
        taskidToTIPMap.put(taskid, tip);
    }
    void removeTaskEntry(String taskid) {
        // taskid --> tracker
        String tracker = (String) taskidToTrackerMap.remove(taskid);

        // tracker --> taskid
        TreeSet trackerSet = (TreeSet) trackerToTaskMap.get(tracker);
        if (trackerSet != null) {
            trackerSet.remove(taskid);
        }

        // taskid --> TIP
        taskidToTIPMap.remove(taskid);
    }

    ///////////////////////////////////////////////////////
    // Accessors for objects that want info on jobs, tasks,
    // trackers, etc.
    ///////////////////////////////////////////////////////
    public int getTotalSubmissions() {
        return totalSubmissions;
    }
    // 本机名
    public String getJobTrackerMachine() {
        return localMachine;
    }
    // 端口
    public int getTrackerPort() {
        return port;
    }
    // info
    public int getInfoPort() {
        return infoPort;
    }
    public long getStartTime() {
        return startTime;
    }

    public Vector runningJobs() {
        Vector v = new Vector();
        // 所有runnning的jobs
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.RUNNING) {
                v.add(jip);
            }
        }
        return v;
    }
    public Vector failedJobs() {
        Vector v = new Vector();
        // 所有失败的job
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.FAILED) {
                v.add(jip);
            }
        }
        return v;
    }
    public Vector completedJobs() {
        Vector v = new Vector();
        // 所有完成的jobs
        for (Iterator it = jobs.values().iterator(); it.hasNext(); ) {
            JobInProgress jip = (JobInProgress) it.next();
            JobStatus status = jip.getStatus();
            if (status.getRunState() == JobStatus.SUCCEEDED) {
                v.add(jip);
            }
        }
        return v;
    }

    public Collection taskTrackers() {
      synchronized (taskTrackers) {
          // 所有task tracker的状态
        return taskTrackers.values();
      }
    }

    // 某个task tracker 的status
    public TaskTrackerStatus getTaskTracker(String trackerID) {
      synchronized (taskTrackers) {
          // task的当前状态
        return (TaskTrackerStatus) taskTrackers.get(trackerID);
      }
    }

    ////////////////////////////////////////////////////
    // InterTrackerProtocol
    ////////////////////////////////////////////////////
    public void initialize(String taskTrackerName) {
      synchronized (taskTrackers) {
          // 清空一下 tasktracker的记录
        boolean seenBefore = updateTaskTrackerStatus(taskTrackerName, null);
        if (seenBefore) {
            // 如果之前有记录的话
          lostTaskTracker(taskTrackerName);
        }
        }
      }
    }

    /**
     * Update the last recorded status for the given task tracker.
     * It assumes that the taskTrackers are locked on entry.
     * @author Owen O'Malley
     * @param trackerName The name of the tracker
     * @param status The new status for the task tracker
     * @return Was an old status found?
     */
    private boolean updateTaskTrackerStatus(String trackerName,
                                            TaskTrackerStatus status) {
        // tasktracker的状态
      TaskTrackerStatus oldStatus = 
        (TaskTrackerStatus) taskTrackers.get(trackerName);
      if (oldStatus != null) {
          // 减掉总共maptask个数
        totalMaps -= oldStatus.countMapTasks();
         // 减掉总共reduce task个数
        totalReduces -= oldStatus.countReduceTasks();
        if (status == null) {
          taskTrackers.remove(trackerName);
        }
      }
      // 使用新状态代替老状态
      if (status != null) {
        totalMaps += status.countMapTasks();
        totalReduces += status.countReduceTasks();
        taskTrackers.put(trackerName, status);
      }
      return oldStatus != null;
    }
    
    /**
     * Process incoming heartbeat messages from the task trackers.
     */
    // tasktracker  发来心跳了
    public synchronized int emitHeartbeat(TaskTrackerStatus trackerStatus, boolean initialContact) {
        // tasktracker 的名字
        String trackerName = trackerStatus.getTrackerName();
        // tasktracker 最后一次见面时间
        trackerStatus.setLastSeen(System.currentTimeMillis());

        synchronized (taskTrackers) {
            synchronized (trackerExpiryQueue) {
                // 之前注册过该tracker吗？
                // 使用发送过来的新状态更新totalmap，totalreduce个数
                boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                             trackerStatus);
                if (initialContact) {
                    // If it's first contact, then clear out any state hanging around
                    if (seenBefore) {
                        lostTaskTracker(trackerName);
                    }
                } else {
                    // seenbefore 应该位true才对，如果不是就有问题了，回复unknown
                    // If not first contact, there should be some record of the tracker
                    if (!seenBefore) {
                        return InterTrackerProtocol.UNKNOWN_TASKTRACKER;
                    }
                }
                // 初次接触
                if (initialContact) {
                    // 加入 expiryqueue
                    trackerExpiryQueue.add(trackerStatus);
                }
            }
        }

        updateTaskStatuses(trackerStatus);
        //LOG.info("Got heartbeat from "+trackerName);

        // 回复成功
        return InterTrackerProtocol.TRACKERS_OK;
    }

    /**
     * A tracker wants to know if there's a Task to run.  Returns
     * a task we'd like the TaskTracker to execute right now.
     *
     * Eventually this function should compute load on the various TaskTrackers,
     * and incorporate knowledge of DFS file placement.  But for right now, it
     * just grabs a single item out of the pending task list and hands it back.
     */
    // tasktracker 过来要task 了

    // 输入一个tasktracker， 传出一个task给他run
    public synchronized Task pollForNewTask(String taskTracker) {
        //
        // Compute average map and reduce task numbers across pool
        //
        int avgMaps = 0;
        int avgReduces = 0;
        int numTaskTrackers;
        TaskTrackerStatus tts;
        synchronized (taskTrackers) {
            // 一共多少个task tracker
          numTaskTrackers = taskTrackers.size();
          // 当前的 tasktracker 的状态
          tts = (TaskTrackerStatus) taskTrackers.get(taskTracker);
        }
        if (numTaskTrackers > 0) {
            // 平均每个 task tracker 分到多少map
          avgMaps = totalMaps / numTaskTrackers;
           // 平均每个 task tracker 分到多少 reduce
          avgReduces = totalReduces / numTaskTrackers;
        }
        // 总共能同时跑多少task
        int totalCapacity = numTaskTrackers * maxCurrentTasks;
        //
        // Get map + reduce counts for the current tracker.
        //
        if (tts == null) {
          LOG.warning("Unknown task tracker polling; ignoring: " + taskTracker);
          return null;
        }
        // 当前tracker 上有多少 map
        int numMaps = tts.countMapTasks();
        // 当前tracker 上有多少 reduce
        int numReduces = tts.countReduceTasks();

        //
        // In the below steps, we allocate first a map task (if appropriate),
        // and then a reduce task if appropriate.  We go through all jobs
        // in order of job arrival; jobs only get serviced if their 
        // predecessors are serviced, too.
        //

        //
        // We hand a task to the current taskTracker if the given machine 
        // has a workload that's equal to or less than the averageMaps 
        // +/- TASK_ALLOC_EPSILON.  (That epsilon is in place in case
        // there is an odd machine that is failing for some reason but 
        // has not yet been removed from the pool, making capacity seem
        // larger than it really is.)
        //
        synchronized (jobsByArrival) {
            // map数量不超过本 tracker 的 task上限，并且不超过平均值
            if ((numMaps < maxCurrentTasks) &&
                (numMaps <= (avgMaps + TASK_ALLOC_EPSILON))) {

                int totalNeededMaps = 0;
                // 根据先来后到，遍历取得第一个running的job，从它里面拿一个map任务
                for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                    JobInProgress job = (JobInProgress) it.next();
                    // job 不在run的状态，就跳过
                    if (job.getStatus().getRunState() != JobStatus.RUNNING) {
                        continue;
                    }
                    // 从这个job里面获取一个map task来run
                    Task t = job.obtainNewMapTask(taskTracker, tts);
                    if (t != null) {
                        return t;
                    }

                    //
                    // Beyond the highest-priority task, reserve a little 
                    // room for failures and speculative executions; don't 
                    // schedule tasks to the hilt.
                    //
                    totalNeededMaps += job.desiredMaps();
                    double padding = 0;
                    if (totalCapacity > MIN_SLOTS_FOR_PADDING) {
                        padding = Math.min(maxCurrentTasks, totalNeededMaps * PAD_FRACTION);
                    }
                    if (totalNeededMaps + padding >= totalCapacity) {
                        break;
                    }
                }
            }

            //
            // Same thing, but for reduce tasks
            //
            // reduce task 小于最大task 数，并且小于平均数
            if ((numReduces < maxCurrentTasks) &&
                (numReduces <= (avgReduces + TASK_ALLOC_EPSILON))) {

                int totalNeededReduces = 0;
                for (Iterator it = jobsByArrival.iterator(); it.hasNext(); ) {
                    JobInProgress job = (JobInProgress) it.next();
                    if (job.getStatus().getRunState() != JobStatus.RUNNING) {
                        continue;
                    }
                    // 选一个reduce 给他
                    Task t = job.obtainNewReduceTask(taskTracker, tts);
                    if (t != null) {
                        return t;
                    }

                    //
                    // Beyond the highest-priority task, reserve a little 
                    // room for failures and speculative executions; don't 
                    // schedule tasks to the hilt.
                    //
                    totalNeededReduces += job.desiredReduces();
                    double padding = 0;
                    if (totalCapacity > MIN_SLOTS_FOR_PADDING) {
                        padding = Math.min(maxCurrentTasks, totalNeededReduces * PAD_FRACTION);
                    }
                    if (totalNeededReduces + padding >= totalCapacity) {
                        break;
                    }
                }
            }
        }
        // 获得不到任何task
        return null;
    }

    /**
     * A tracker wants to know if any of its Tasks have been
     * closed (because the job completed, whether successfully or not)
     */
    // 返回任意一个taskId， 它因为job的结束，自己也需要结束
    public synchronized String pollForTaskWithClosedJob(String taskTracker) {
        // tracker 下的所有taskId
        TreeSet taskIds = (TreeSet) trackerToTaskMap.get(taskTracker);
        if (taskIds != null) {
            for (Iterator it = taskIds.iterator(); it.hasNext(); ) {
                // task id
                String taskId = (String) it.next();
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);
                // 因为job结束了，所以taskId也相应被关
                if (tip.shouldCloseForClosedJob(taskId)) {
                    // 
                    // This is how the JobTracker ends a task at the TaskTracker.
                    // It may be successfully completed, or may be killed in
                    // mid-execution.
                    //
                    return taskId;
                }
            }
        }
        return null;
    }

    /**
     * A TaskTracker wants to know the physical locations of completed, but not
     * yet closed, tasks.  This exists so the reduce task thread can locate
     * map task outputs.
     */
    // reduce task 获取map task 输出的位置
    public synchronized MapOutputLocation[] locateMapOutputs(String taskId, String[][] mapTasksNeeded) {
        ArrayList v = new ArrayList();
        for (int i = 0; i < mapTasksNeeded.length; i++) {
            // 遍历所有需要的map task
            for (int j = 0; j < mapTasksNeeded[i].length; j++) {
                // 使用task id获得task in progress
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(mapTasksNeeded[i][j]);
                // 是否完结了
                if (tip != null && tip.isComplete(mapTasksNeeded[i][j])) {
                    // 获得 task 所在tracker的id
                    String trackerId = (String) taskidToTrackerMap.get(mapTasksNeeded[i][j]);
                    TaskTrackerStatus tracker;
                    synchronized (taskTrackers) {
                        // 获得tracker的状态
                      tracker = (TaskTrackerStatus) taskTrackers.get(trackerId);
                    }
                    // map 的 taskid 和 tracker的host 和 port
                    v.add(new MapOutputLocation(mapTasksNeeded[i][j], tracker.getHost(), tracker.getPort()));
                    break;
                }
            }
        }
        // randomly shuffle results to load-balance map output requests
        // 将 location 打乱
        Collections.shuffle(v);

        return (MapOutputLocation[]) v.toArray(new MapOutputLocation[v.size()]);
    }

    /**
     * Grab the local fs name
     */
    public synchronized String getFilesystemName() throws IOException {
        // 文件系统名称
        return fs.getName();
    }

    ////////////////////////////////////////////////////
    // JobSubmissionProtocol
    ////////////////////////////////////////////////////
    /**
     * JobTracker.submitJob() kicks off a new job.  
     *
     * Create a 'JobInProgress' object, which contains both JobProfile
     * and JobStatus.  Those two sub-objects are sometimes shipped outside
     * of the JobTracker.  But JobInProgress adds info that's useful for
     * the JobTracker alone.
     *
     * We add the JIP to the jobInitQueue, which is processed 
     * asynchronously to handle split-computation and build up
     * the right TaskTracker/Block mapping.
     */
    public synchronized JobStatus submitJob(String jobFile) throws IOException {
        // 又提交了一个
        totalSubmissions++;

        JobInProgress job = new JobInProgress(jobFile, this, this.conf);
        synchronized (jobs) {
            synchronized (jobsByArrival) {
                synchronized (jobInitQueue) {
                    // 放入 jobid   和对应的 job
                    jobs.put(job.getProfile().getJobId(), job);

                    jobsByArrival.add(job);

                    // 放入任务队列
                    jobInitQueue.add(job);
                    // 唤醒消费者们去拿
                    jobInitQueue.notifyAll();
                }
            }
        }
        // 获取任务的状态
        return job.getStatus();
    }

    public synchronized ClusterStatus getClusterStatus() {
        synchronized (taskTrackers) {
            // 获取cluster的状态
          return new ClusterStatus(taskTrackers.size(),
                                   totalMaps,
                                   totalReduces,
                                   maxCurrentTasks);          
        }
    }
    
    public synchronized void killJob(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        // 杀掉那个job
        job.kill();
    }

    public synchronized JobProfile getJobProfile(String jobid) {
        // 根据jobid拿到job in progrss 的 profile
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getProfile();
        } else {
            return null;
        }
    }
    public synchronized JobStatus getJobStatus(String jobid) {
        // 根据jobid拿到job in progrss 的状态
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job != null) {
            return job.getStatus();
        } else {
            return null;
        }
    }
    public synchronized TaskReport[] getMapTaskReports(String jobid) {
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job == null) {
            return new TaskReport[0];
        } else {
            Vector reports = new Vector();
            Vector completeMapTasks = job.reportTasksInProgress(true, true);
            // 获得所有完成的taskinprocess
            for (Iterator it = completeMapTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                // task in progess的瞬时静态report
                reports.add(tip.generateSingleReport());
            }
            Vector incompleteMapTasks = job.reportTasksInProgress(true, false);
            // 获得所有未完成的taskinprogress
            for (Iterator it = incompleteMapTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                // task in progess的瞬时静态report
                reports.add(tip.generateSingleReport());
            }
            return (TaskReport[]) reports.toArray(new TaskReport[reports.size()]);
        }
    }

    public synchronized TaskReport[] getReduceTaskReports(String jobid) {
        // 获取job in progress 对象
        JobInProgress job = (JobInProgress) jobs.get(jobid);
        if (job == null) {
            return new TaskReport[0];
        } else {
            // 获取report
            Vector reports = new Vector();
            Vector completeReduceTasks = job.reportTasksInProgress(false, true);
            // 所有完成的reducetasks
            for (Iterator it = completeReduceTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                // task in progess的瞬时静态report
                reports.add(tip.generateSingleReport());
            }
            // 所有完成的reducetasks
            Vector incompleteReduceTasks = job.reportTasksInProgress(false, false);
            for (Iterator it = incompleteReduceTasks.iterator(); it.hasNext(); ) {
                TaskInProgress tip = (TaskInProgress) it.next();
                // task in progress的瞬时静态report
                reports.add(tip.generateSingleReport());
            }
            return (TaskReport[]) reports.toArray(new TaskReport[reports.size()]);
        }
    }

    ///////////////////////////////////////////////////////////////
    // JobTracker methods
    ///////////////////////////////////////////////////////////////
    // 根据id获得jobinprocess
    public JobInProgress getJob(String jobid) {
        return (JobInProgress) jobs.get(jobid);
    }
    /**
     * Grab random num for task id
     */
    String createUniqueId() {
        return "" + Integer.toString(Math.abs(r.nextInt()),36);
    }

    ////////////////////////////////////////////////////
    // Methods to track all the TaskTrackers
    ////////////////////////////////////////////////////
    /**
     * Accept and process a new TaskTracker profile.  We might
     * have known about the TaskTracker previously, or it might
     * be brand-new.  All task-tracker structures have already
     * been updated.  Just process the contained tasks and any
     * jobs that might be affected.
     */
    // 一个task tracker的状态
    void updateTaskStatuses(TaskTrackerStatus status) {
        // 通过tasktracker获得所有taskstatus
        for (Iterator it = status.taskReports(); it.hasNext(); ) {
            // task状态
            TaskStatus report = (TaskStatus) it.next();
            TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(report.getTaskId());
            if (tip == null) {
                LOG.info("Serious problem.  While updating status, cannot find taskid " + report.getTaskId());
            } else {
                // 根据task获得所在的job
                JobInProgress job = tip.getJob();
                // 先更新task in progress的状态，在更新job in progress的状态
                job.updateTaskStatus(tip, report);

                if (report.getRunState() == TaskStatus.SUCCEEDED) {
                    job.completedTask(tip, report.getTaskId());
                } else if (report.getRunState() == TaskStatus.FAILED) {
                    // Tell the job to fail the relevant task
                    job.failedTask(tip, report.getTaskId(), status.getTrackerName());
                }
            }
        }
    }

    /**
     * We lost the task tracker!  All task-tracker structures have 
     * already been updated.  Just process the contained tasks and any
     * jobs that might be affected.
     */
    void lostTaskTracker(String trackerName) {
        // tasktracker重连上来的时候，报一下lost
        LOG.info("Lost tracker '" + trackerName + "'");
        // 丢失的任务们，把他们所属的job fail掉
        TreeSet lostTasks = (TreeSet) trackerToTaskMap.get(trackerName);
        trackerToTaskMap.remove(trackerName);

        if (lostTasks != null) {
            for (Iterator it = lostTasks.iterator(); it.hasNext(); ) {
                // 任务们的id
                String taskId = (String) it.next();
                // 任务进度
                TaskInProgress tip = (TaskInProgress) taskidToTIPMap.get(taskId);

                // Tell the job to fail the relevant task
                // 所在job的进度
                JobInProgress job = tip.getJob();
                // 所在job被fail掉
                job.failedTask(tip, taskId, trackerName);
            }
        }
    }

    ////////////////////////////////////////////////////////////
    // main()
    ////////////////////////////////////////////////////////////

    /**
     * Start the JobTracker process.  This is used only for debugging.  As a rule,
     * JobTracker should be run as part of the DFS Namenode process.
     */
    // jobtracker 启动在namenode上
    public static void main(String argv[]) throws IOException, InterruptedException {
        if (argv.length != 0) {
          System.out.println("usage: JobTracker");
          System.exit(-1);
        }

        startTracker(new Configuration());
    }
}
