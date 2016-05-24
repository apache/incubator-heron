package com.twitter.heron.scheduler.mesos.framework.driver;

import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseTask;
import com.twitter.heron.scheduler.mesos.framework.jobs.TaskUtils;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MesosJobFramework implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosJobFramework.class.getName());

  // The port pattern should be "{{task.ports[...]}}" literally
  public static final String portVariableTemplate = "\\{\\{task\\.ports\\[.*?\\]\\}\\}";
  public static final Pattern portVariablePattern = Pattern.compile(portVariableTemplate);

  private final FrameworkConfiguration config;
  private final MesosTaskBuilder taskBuilder;
  private final PersistenceStore persistenceStore;

  private final Map<String, BaseJob> jobNameMapping = new ConcurrentHashMap<>();

  // Containing taskId
  private final Queue<String> pendingScheduleTasks = new LinkedTransferQueue<>();

  private final Map<String, BaseTask> trackingTasks = new HashMap<>();

  // Two CountDownLatch used for notification
  private volatile CountDownLatch startLatch = new CountDownLatch(1);
  private volatile CountDownLatch endLatch = null;

  private SchedulerDriver driver;

  // For failure recovery
  // ----------------
  private volatile boolean isRecovered = false;
  // This would be updated by main thread and mesos slave thread, synchronized...
  // From taskId -> TaskStatus
  private final Map<String, Protos.TaskStatus> remaining = new HashMap<>();
  private final ScheduledExecutorService reconcileExecutors = Executors.newScheduledThreadPool(1);
  // Time in seconds since the Epoch. Make it this way to match mesos
  private final double startTime = ((double) System.currentTimeMillis()) / 1000;

  public MesosJobFramework(MesosTaskBuilder taskBuilder,
                           PersistenceStore persistenceStore,
                           FrameworkConfiguration config) {
    this.taskBuilder = taskBuilder;
    this.persistenceStore = persistenceStore;
    this.config = config;
  }

  public void clear() {

  }

  // Used to sync with other threads
  public void setStartNotification(CountDownLatch latch) {
    this.startLatch = latch;
  }

  // Used to sync with other threads
  public void setEndNotification(CountDownLatch latch) {
    this.endLatch = latch;
  }

  public synchronized boolean setSafeLatch(CountDownLatch latch) {
    if (!isRecovered) {
      this.startLatch = latch;
      return true;
    }

    return false;
  }

  private synchronized void safeSetRecovered() {
    isRecovered = true;

    if (startLatch != null) {
      startLatch.countDown();
    }
  }

  public void reconcileTasks(final SchedulerDriver schedulerDriver) {
    LOG.info("Starting reconcile executor.");
    // First set up
    for (BaseTask task : trackingTasks.values()) {
      BaseTask.TaskState state = task.state;

      if (state.equals(BaseTask.TaskState.SCHEDULED) ||
          state.equals(BaseTask.TaskState.TO_KILL) ||
          state.equals(BaseTask.TaskState.TO_KILL_NOW)) {
        remaining.put(
            task.taskId,
            TaskUtils.getMesosTaskStatus(task, Protos.TaskState.TASK_ERROR).build());
      }
    }

    if (!isRecovered && remaining.isEmpty()) {
      LOG.info("No tasks need to reconcile.");

      safeSetRecovered();
    }

    Runnable r = new Runnable() {
      @Override
      public void run() {
        synchronized (this) {
          if (!remaining.isEmpty()) {
            LOG.info("We are doing explicit reconciliation to recover from failure");

            schedulerDriver.reconcileTasks(remaining.values());
          } else {
            LOG.info("We are doing implicit reconciliation to check health periodically");

            schedulerDriver.reconcileTasks(new LinkedList<Protos.TaskStatus>());
          }

          reconcileExecutors.schedule(this, config.reconciliationIntervalInMs, TimeUnit.MILLISECONDS);
        }
      }
    };

    // First time, the entry, starts immediately
    reconcileExecutors.schedule(r, 0, TimeUnit.MILLISECONDS);
  }

  // Load jobs from failure
  public void loadJobs(Iterable<BaseJob> jobs) {
    for (BaseJob job : jobs) {
      jobNameMapping.put(job.name, job);
    }
  }

  // Load trackingTasks from failure
  public void loadTasks(Map<String, BaseTask> tasks) {
    trackingTasks.putAll(tasks);
  }

  public void loadPendingScheduleTasks() {
    for (BaseTask task : trackingTasks.values()) {
      if (task.state.equals(BaseTask.TaskState.TO_SCHEDULE)) {
        pendingScheduleTasks.add(task.taskId);
      }
    }
  }

  public boolean isReady() {
    return isRecovered;
  }

  public boolean scheduleNewJob(BaseJob job) {
    synchronized (this) {
      if (jobNameMapping.containsKey(job.name)) {
        LOG.severe("The job is still on-tracked, please kill it first:" + job.name);
        return false;
      }
      jobNameMapping.put(job.name, job);

      return scheduleNewTask(TaskUtils.getTaskId(job, System.currentTimeMillis(), 0));
    }
  }

  // We could make sure the job has run earlier
  public boolean removeScheduledJob(String jobName, MesosDriverFactory mesosDriver, boolean killJobNow) {
    synchronized (this) {
      LOG.info(String.format("Kill job: %s, killNow: %s", jobName, killJobNow));

      BaseTask task = trackingTasks.get(jobName);
      if (task == null) {
        LOG.info("The job is not on-tracked.");
        LOG.info("Current trackingTasks: " + trackingTasks.toString());

        return true;
      }
      String taskId = task.taskId;
      LOG.info("The info of task to remove: " + task.toString());

      BaseTask.TaskState oldState = task.state;
      LOG.info(String.format("The old is %s before killing", oldState));

      // WAL first
      if (killJobNow) {
        task.state = BaseTask.TaskState.TO_KILL_NOW;
      } else {
        task.state = BaseTask.TaskState.TO_KILL;
      }
      updateTrackingTask(jobName, task);

      removePendingTask(taskId);
      LOG.info(String.format("Removed task: %s from pendingScheduleTasks[pend-to-schedule queue]", taskId));

      // Just send a reconcile and handle the logic in statusUpdate, more uniformly
      mesosDriver.get().reconcileTasks(
          Collections.singletonList(
              TaskUtils.getMesosTaskStatus(task, Protos.TaskState.TASK_ERROR).build()));

      return true;
    }
  }

  public boolean restartScheduledJob(String jobName, MesosDriverFactory mesosDriver) {
    // We just need to kill that job in mesos cluster
    // MesosJobFramework would restart that job automatically
    LOG.info("Killing mesos job: " + jobName);

    BaseTask task = trackingTasks.get(jobName);
    if (task == null) {
      LOG.info("The job is not running");
      return false;
    }

    mesosDriver.get().
        killTask(Protos.TaskID.newBuilder().setValue(task.taskId).build());

    return true;
  }

  @Override
  public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
    LOG.info("Registered with ID: " + frameworkId.getValue());
    LOG.info("Master info: " + masterInfo.toString());

    //store the frameworkId
    persistenceStore.persistFrameworkID(frameworkId);


    LOG.info("Start to reconcile all tasks under monitoring");
    reconcileTasks(driver);
    this.driver = driver;
  }

  @Override
  public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
    LOG.info("Re-registered");

    LOG.info("Start to reconcile all tasks under monitoring");
    reconcileTasks(driver);
    this.driver = driver;
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
    synchronized (this) {
      LOG.fine("Received Resource Offers");

      if (!isReady()) {
        LOG.info("Not finished reconciliation, reclining offers.");
        return;
      }

      Map<Protos.Offer, TaskResources> offerResources = new HashMap<>();
      for (Protos.Offer offer : offers) {
        offerResources.put(offer, TaskResources.apply(offer, config.role));
      }

      // Only try to schedule jobs when isReady() == true
      List<LaunchableTasks> tasksToLaunch = isReady() ?
          generateLaunchableTasks(offerResources) : new LinkedList<LaunchableTasks>();

      LOG.fine("Declining unused offers.");
      Set<String> usedOffers = new HashSet<>();
      for (LaunchableTasks task : tasksToLaunch) {
        usedOffers.add(task.offer.getId().getValue());
      }
      for (Protos.Offer offer : offers) {
        if (!usedOffers.contains(offer.getId().getValue())) {
          driver.declineOffer(offer.getId());
        }
      }

      launchTasks(driver, tasksToLaunch);
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
    LOG.info("Offer rescinded for offer:" + offerId.getValue());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
    // CONSIDER TASK_TO_KILL and TASK_TO_KILL_NOW is a state we monitor and terminal state.
    // We do not monitor other terminal states.
    synchronized (this) {
      LOG.info(String.format("Received status update [%s]", status));
      // For start reconcile
      if (!remaining.isEmpty()) {
        String taskId = status.getTaskId().getValue();
        double timestamp = status.getTimestamp();

        if (remaining.containsKey(taskId) && timestamp > startTime) {
          remaining.remove(taskId);
        }
      }

      // No remaining, release the flag
      if (!isRecovered && remaining.isEmpty()) {
        LOG.info("All tasks are reconciled! Release the flag!");
        safeSetRecovered();
      }


      String taskId = status.getTaskId().getValue();
      String jobName = TaskUtils.getJobNameForTaskId(taskId);

      if (jobNameMapping.get(jobName) == null) {
        LOG.info("Job no longer registered (might be killed earlier): " + jobName);
      }

      BaseTask task = trackingTasks.get(jobName);
      if (task == null) {
        LOG.info("Received a status update from unknown task. Possibly client requested a kill and let it finish rather than kill now.");
      } else {
        handleMesosStatusUpdate(driver, task, status);
      }
    }
  }

  private void handleMesosStatusUpdate(SchedulerDriver driver, BaseTask task, Protos.TaskStatus status) {
    // We would determine what to do basing on current task and mesos status update
    switch (task.state) {
      case TO_SCHEDULE:
        LOG.info(String.format("Received [%s] update for a task in TO_SCHEDULE state: [%s]", status, task));
        break;
      case SCHEDULED:
        switch (status.getState()) {
          case TASK_STAGING:
            LOG.info(String.format("Task with id '%s' STAGING", status.getTaskId().getValue()));
            LOG.severe("Framework status updates should not use");

            break;
          case TASK_STARTING:
            LOG.info(String.format("Task with id '%s' STARTING", status.getTaskId().getValue()));

            break;
          case TASK_RUNNING:
            LOG.info(String.format("Task with id '%s' RUNNING", status.getTaskId().getValue()));

            break;
          case TASK_FINISHED:
            LOG.info(String.format("Task with id '%s' FINISHED", status.getTaskId().getValue()));

            task.state = BaseTask.TaskState.FINISHED_SUCCESS;
            updateTrackingTask(TaskUtils.getJobNameForTaskId(task.taskId), task);
            break;
          case TASK_FAILED:
            LOG.info(String.format("Task with id '%s' FAILED", status.getTaskId().getValue()));
            handleMesosFailure(task);

            break;
          case TASK_KILLED:
            LOG.info("Task killed which is supported running. It could be triggered by manual restart command");
            handleMesosFailure(task);

            break;
          case TASK_LOST:
            LOG.info(String.format("Task with id '%s' LOST", status.getTaskId().getValue()));
            handleMesosFailure(task);

            break;
          case TASK_ERROR:
            LOG.info(String.format("Task with id '%s' ERROR", status.getTaskId().getValue()));
            handleMesosFailure(task);

            break;
          default:
            LOG.severe("Unknown TaskState:" + status.getState() + " for task: " + status.getTaskId().getValue());

            break;
        }

        break;
      case TO_KILL:
        if (status.getState().equals(Protos.TaskState.TASK_STAGING) ||
            status.getState().equals(Protos.TaskState.TASK_STARTING) ||
            status.getState().equals(Protos.TaskState.TASK_RUNNING)) {
          LOG.info(
              String.format("The job is still running in status: %s", status.getState()));
          LOG.info("Do nothing and continue to wait for its NON_RUNNING");
        } else {
          LOG.info(
              String.format("The job is NOT_RUNNING any more with status: %s", status.getState()));
          LOG.info("The job is supposed to kill. Clear all the states now");
          clearTaskState(task.taskId);
        }
        break;
      case TO_KILL_NOW:
        if (status.getState().equals(Protos.TaskState.TASK_STAGING) ||
            status.getState().equals(Protos.TaskState.TASK_STARTING) ||
            status.getState().equals(Protos.TaskState.TASK_RUNNING)) {
          // We are requested to kill immediately
          LOG.info("The job is still running, kill it now....");
          // Kill it again
          driver.
              killTask(Protos.TaskID.newBuilder().setValue(status.getTaskId().getValue()).build());
        } else {
          LOG.info("The job is requested to kill and now it has been killed already.");
          clearTaskState(task.taskId);
        }
        break;
      // Terminal states
      case FINISHED_SUCCESS:
        // We could not throw exceptions here, though they should not be states related to mesos directly,
        // considering there might be a lot of pending statusUpdate coming at the same time
        LOG.info(String.format("Received [%s] update for a task in terminated state: [%s]", status, task));
        break;
      case FINISHED_FAILURE:
        LOG.info(String.format("Received [%s] update for a task in terminated state: [%s]", status, task));
        break;
      default:
        LOG.info(String.format("Received [%s] update for task in unknown state: [%s]", status, task));
    }
  }

  private void handleMesosFailure(BaseTask task) {
    String taskId = task.taskId;
    String jobName = TaskUtils.getJobNameForTaskId(taskId);
    int attempt = TaskUtils.getAttemptForTaskId(taskId);
    BaseJob job = jobNameMapping.get(jobName);

    boolean hasAttemptsLeft = attempt < job.retries;

    if (hasAttemptsLeft) {
      LOG.warning(String.format("Retrying job: %s, attempt: %d", jobName, attempt + 1));

      scheduleNewTask(TaskUtils.getTaskId(job, System.currentTimeMillis(), attempt + 1));
    } else {
      LOG.info("Would not restart the job since it is beyond retries: " + attempt);
      task.state = BaseTask.TaskState.FINISHED_FAILURE;
      updateTrackingTask(TaskUtils.getJobNameForTaskId(task.taskId), task);
    }
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
    LOG.info("Framework message received");
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    LOG.info("Disconnected");
  }

  @Override
  public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
    LOG.info(String.format("Slave %s lost", slaveId));

    // No need to specifically handle this case
    // since framework would receive TASK_LOST status update for every task on that Slave
  }

  @Override
  public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
    LOG.info("Executor lost");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    LOG.severe(message);

    System.exit(1);
  }

  /**
   * -------------------------------------
   * -------------------------------------
   * Following are private methods
   * -------------------------------------
   * -------------------------------------
   */

  private boolean scheduleNewTask(String taskId) {
    // Put it to the pending start queue
    LOG.info(String.format("We are to schedule task: [%s], set it TO_SCHEDULE. WAL", taskId));
    BaseTask task = new BaseTask(taskId, "not-yet-scheduled", BaseTask.TaskState.TO_SCHEDULE);
    updateTrackingTask(TaskUtils.getJobNameForTaskId(taskId), task);

    pendingScheduleTasks.add(taskId);
    LOG.info(String.format("Added task: %s into the pending-to-start-queue: ", taskId));
    return true;
  }

  private void removePendingTask(String taskId) {
    // Remove it the task queue
    Iterator<String> it = pendingScheduleTasks.iterator();
    while (it.hasNext()) {
      if (it.next().equals(taskId)) {
        it.remove();
        break;
      }
    }
  }

  private void clearTaskState(String taskId) {
    LOG.info("To clear all states for task: " + taskId);
    String jobName = TaskUtils.getJobNameForTaskId(taskId);
    // Clear the state in memory
    jobNameMapping.remove(jobName);
    trackingTasks.remove(jobName);

    persistenceStore.removeJob(jobName);
    persistenceStore.removeTask(taskId);

    if (endLatch != null) {
      endLatch.countDown();
    }
  }

  // Update structure in memory, do the WAL and then it is safe to ack back to the SLAVE in mesos
  private void updateTrackingTask(String jobName, BaseTask task) {
    LOG.info(String.format("Updating status for job: %s, to %s",
        jobName, task));

    // WAL log
    persistenceStore.persistTask(jobName, task);

    // Push back
    trackingTasks.put(jobName, task);
  }

  private List<LaunchableTasks> generateLaunchableTasks(Map<Protos.Offer, TaskResources> offerResources) {
    List<LaunchableTasks> tasks = new LinkedList<>();
    Set<String> taskIds = new HashSet<>();

    while (!pendingScheduleTasks.isEmpty()) {
      String taskId = pendingScheduleTasks.poll();
      BaseJob job = jobNameMapping.get(TaskUtils.getJobNameForTaskId(taskId));

      BaseTask task = trackingTasks.get(job.name);
      if (task != null && task.state.equals(BaseTask.TaskState.SCHEDULED)) {
        LOG.info("Found job in queue that is already scheduled and running for launch: " + job.name);

      } else {
        if (taskIds.contains(taskId)) {
          LOG.info("Found job in queue that is already scheduled for launch with this offer set: " + job.name + "\n");

        } else {
          TaskResources neededResources = new TaskResources(job);

          Iterator<Map.Entry<Protos.Offer, TaskResources>> it = offerResources.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<Protos.Offer, TaskResources> kv = it.next();
            Protos.Offer offer = kv.getKey();
            TaskResources resources = kv.getValue();

            if (resources.canSatisfy(neededResources)) {
              // TODO(mfu): also need to check whether we meet the constraints
              resources.consume(neededResources);
              tasks.add(new LaunchableTasks(taskId, job, offer,
                  (int) (neededResources.portsHold.get(0).rangeStart),
                  (int) (neededResources.portsHold.get(0).rangeEnd)));
              taskIds.add(taskId);

              // Matched task, break;
              break;
            } else {
              LOG.info(String.format("Insufficient resources remaining for task: %s, will append to queue. Need: [%s], Found: [%s]",
                  taskId, neededResources.toString(), resources.toString()));

              pendingScheduleTasks.add(taskId);

              // Exit this method
              return tasks;
            }
          }
        }
      }
    }

    return tasks;
  }

  public void killFramework() {
    driver.stop();
  }

  private void launchTasks(SchedulerDriver driver, List<LaunchableTasks> tasks) {
    // Group by
    Map<Protos.Offer, List<LaunchableTasks>> tasksGroupByOffer = new HashMap<>();
    for (LaunchableTasks task : tasks) {
      List<LaunchableTasks> subTasks = tasksGroupByOffer.get(task.offer);
      if (subTasks == null) {
        subTasks = new LinkedList<>();
      }
      subTasks.add(task);
      tasksGroupByOffer.put(task.offer, subTasks);
    }

    for (Map.Entry<Protos.Offer, List<LaunchableTasks>> kv : tasksGroupByOffer.entrySet()) {
      Protos.Offer offer = kv.getKey();
      List<LaunchableTasks> subTasks = kv.getValue();

      List<Protos.TaskInfo> mesosTasks = new LinkedList<>();
      for (LaunchableTasks task : subTasks) {
        Protos.TaskInfo.Builder mesosTask = taskBuilder.
            getMesosTaskInfoBuilder(task.taskId, task.baseJob, task.offer).
            addResources(
                taskBuilder.rangeResource(
                    MesosTaskBuilder.portResourceName, task.portRangeStart, task.portRangeEnd, offer)).
            setSlaveId(task.offer.getSlaveId());

        String convertedCommand =
            allocatePortsInCommand(task.baseJob.command, task.portRangeStart, task.portRangeEnd);
        mesosTask.setCommand(mesosTask.getCommandBuilder().setValue(convertedCommand));

        mesosTasks.add(
            mesosTask.build());
      }

      // WAL
      for (LaunchableTasks task : tasks) {
        BaseTask taskToPersist =
            new BaseTask(task.taskId,
                task.offer.getSlaveId().getValue(),
                BaseTask.TaskState.SCHEDULED);

        LOG.info(String.format("Ready to launch task %s. WAL", task.taskId));
        updateTrackingTask(task.baseJob.name, taskToPersist);
      }

      LOG.info("Launching tasks from offer: " + offer + " with tasks: " + mesosTasks);

      Protos.Status status =
          driver.launchTasks(Arrays.asList(new Protos.OfferID[]{offer.getId()}),
              mesosTasks);

      if (status == Protos.Status.DRIVER_RUNNING) {
        LOG.info(String.format("Tasks launched, status: '%s'", status));

      } else {
        LOG.info("Other status returned: " + status);
      }
    }
  }

  /**
   * -------------------------------------
   * -------------------------------------
   * Following are static class
   * -------------------------------------
   * -------------------------------------
   */

  static class LaunchableTasks {
    public final String taskId;
    public final BaseJob baseJob;
    public final Protos.Offer offer;
    public final int portRangeStart;
    public final int portRangeEnd;


    public LaunchableTasks(String taskId, BaseJob baseJob, Protos.Offer offer,
                           int portRangeStart, int portRangeEnd) {
      this.taskId = taskId;
      this.baseJob = baseJob;
      this.offer = offer;
      this.portRangeStart = portRangeStart;
      this.portRangeEnd = portRangeEnd;
    }
  }

  public static class TaskResources {
    public static class Range {
      public long rangeStart;
      public long rangeEnd;

      public Range(long rangeStart, long rangeEnd) {
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
      }
    }

    private double cpu;
    private double mem;
    private double disk;
    // # of ports request
    private int ports;

    private List<Range> portsHold = new ArrayList<>();

    public TaskResources(double cpu, double mem, double disk, List<Range> portsResources) {
      this.cpu = cpu;
      this.mem = mem;
      this.disk = disk;
      this.ports = 0;
      this.portsHold = portsResources;
    }

    public TaskResources(BaseJob job) {
      this.cpu = job.cpu;
      this.mem = job.mem;
      this.disk = job.disk;
      this.ports = getRequiredPorts(job.command);
    }

    public boolean canSatisfy(TaskResources needed) {
      // First let us check whether ports meet the requirement
      boolean isPortsEnough = false;
      for (Range portRange : portsHold) {
        if (portRange.rangeEnd - portRange.rangeStart + 1 > needed.ports) {
          isPortsEnough = true;
          break;
        }
      }

      return isPortsEnough &&
          (this.cpu >= needed.cpu) &&
          (this.mem >= needed.mem) &&
          (this.disk >= needed.disk);
    }

    public void consume(TaskResources needed) {
      this.cpu -= needed.cpu;
      this.mem -= needed.mem;
      this.disk -= needed.disk;

      // Consume the port
      for (Range portRange : portsHold) {
        if (portRange.rangeEnd - portRange.rangeStart + 1 > needed.ports) {
          // Give the ports to the needed guy
          needed.portsHold.add(new Range(portRange.rangeStart, portRange.rangeStart + needed.ports - 1));
          // And then consume us by update the range value
          portRange.rangeStart = portRange.rangeStart + needed.ports;

          // Done. Break the loop
          break;
        }
      }

    }

    public String toString() {
      return String.format("cpu: %s; mem: %s; disk: %s.", this.cpu, this.mem, this.disk);
    }

    public static TaskResources apply(Protos.Offer offer, String role) {
      double cpu = 0;
      double mem = 0;
      double disk = 0;
      List<Range> portsResource = new ArrayList<>();
      for (Protos.Resource r : offer.getResourcesList()) {
        if (!r.hasRole() || r.getRole().equals("*") || r.getRole().equals(role)) {
          if (r.getName().equals("cpus")) {
            cpu = getScalarValue(r);
          }
          if (r.getName().equals("mem")) {
            mem = getScalarValue(r);
          }
          if (r.getName().equals("disk")) {
            disk = getScalarValue(r);
          }

          if (r.getName().equals("ports")) {
            Protos.Value.Ranges ranges = r.getRanges();
            for (Protos.Value.Range range : ranges.getRangeList()) {
              portsResource.add(new Range(range.getBegin(), range.getEnd()));
            }
          }
        }
      }

      return new TaskResources(cpu, mem, disk, portsResource);
    }
  }

  static double getScalarValue(Protos.Resource resource) {
    return resource.getScalar().getValue();
  }

  // TODO(mfu): Make it more efficient
  static String allocatePortsInCommand(String originalCommand, int portRangeStart, int portRangeEnd) {
    String result = originalCommand;

    // First figure out all the variables
    Matcher matcher = portVariablePattern.matcher(result);
    Set<String> matchedPortNames = new HashSet<>();
    while (matcher.find()) {
      matchedPortNames.add(matcher.group(0));
    }

    LOG.info("The matchedPortNames: \n" + matchedPortNames);
    LOG.info("The port range: " + portRangeStart + " ~ " + portRangeEnd);

    // Then replace the variable one by once
    // We do this to recognize the same variable
    for (String name : matchedPortNames) {
      if (portRangeStart > portRangeEnd) {
        throw new RuntimeException("The command requires more ports than expected: " + originalCommand);
      }

      result = result.replace(name, "" + portRangeStart);
      portRangeStart++;
    }

    return result;
  }

  static int getRequiredPorts(String command) {
    Matcher matcher = portVariablePattern.matcher(command);
    Set<String> matchedPortNames = new HashSet<>();
    while (matcher.find()) {
      matchedPortNames.add(matcher.group(0));
    }
    return matchedPortNames.size();
  }
}
