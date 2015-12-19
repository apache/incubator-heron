package com.twitter.heron.scheduler.mesos.framework.jobs;

import org.apache.mesos.Protos;

public class TaskUtils {
  //TaskIdFormat: task:JOB_NAME:DUE:ATTEMPT
  public static final String taskIdTemplate = "task:%s:%d:%d";

  public static Protos.TaskStatus.Builder getMesosTaskStatus(BaseTask task, Protos.TaskState state) {
    Protos.TaskStatus.Builder status =
        Protos.TaskStatus.newBuilder().
            setTaskId(Protos.TaskID.newBuilder().setValue(task.taskId)).
            setSlaveId(Protos.SlaveID.newBuilder().setValue(task.slaveId));

    status.setState(state);

    return status;
  }

  public static String getTaskId(BaseJob job, long due, int attempt) {
    return String.format(taskIdTemplate, job.name, due, attempt);
  }

  public static boolean isValidTaskId(String taskId) {
    String[] info = taskId.split(":");
    return info.length == 4;
  }

  public static String getJobNameForTaskId(String taskId) {
    return taskId.split(":")[1];
  }

  public static long getDueTimesForTaskId(String taskId) {
    return Long.parseLong(taskId.split(":")[2]);
  }

  public static int getAttemptForTaskId(String taskId) {
    return Integer.parseInt(taskId.split(":")[3]);
  }
}
