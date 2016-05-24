package com.twitter.heron.scheduler.mesos.framework.state;

import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseTask;
import com.twitter.heron.scheduler.mesos.framework.jobs.TaskUtils;
import org.apache.mesos.Protos;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FilePersistenceStore implements PersistenceStore {
  private static final Logger LOG = Logger.getLogger(FilePersistenceStore.class.getName());

  private final String rootPath;
  private final String frameworkIdPath;
  private final String jobPath;
  private final String taskPath;

  public FilePersistenceStore(String rootPath) {
    this.rootPath = rootPath;
    this.frameworkIdPath = rootPath + "/frameworkId";
    this.jobPath = rootPath + "/jobs";
    this.taskPath = rootPath + "/tasks";
  }

  @Override
  public boolean persistJob(BaseJob baseJob) {
    try {
      LOG.info("Persist Job: " + BaseJob.getJobDefinitionInJSON(baseJob));
      Files.write(
          new File(jobPath + "/" + baseJob.name).toPath(), BaseJob.getJobDefinitionInJSON(baseJob).getBytes());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to write file", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean persistTask(String name, BaseTask task) {
    LOG.info(String.format("Persist Task: %s with details: %s", name, task));

    try {
      Files.write(
          new File(taskPath + "/" + name).toPath(), BaseTask.getTaskInJSON(task).getBytes());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to write file", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean removeTask(String taskId) {
    LOG.info("Remove Task: " + taskId);

    try {
      Files.deleteIfExists(new File(taskPath + "/" + TaskUtils.getJobNameForTaskId(taskId)).toPath());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to delete file", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean removeJob(String jobName) {
    LOG.info("Remove Job: " + jobName);
    try {
      Files.deleteIfExists(new File(jobPath + "/" + jobName).toPath());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to delete file", e);
      return false;
    }

    return true;
  }

  @Override
  public Map<String, BaseTask> getTasks() {
    LOG.info("Get all Tasks");
    File[] files = getFilesInDir(taskPath);

    Map<String, BaseTask> tasks = new HashMap<>();
    for (File file : files) {
      byte[] res;
      try {
        res = Files.readAllBytes(file.toPath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to read from file. ", e);
      }
      String taskDefinitionInJSON = new String(res);
      LOG.info("Def: " + taskDefinitionInJSON);
      BaseTask task = BaseTask.getTaskFromJSONString(taskDefinitionInJSON);

      tasks.put(TaskUtils.getJobNameForTaskId(task.taskId), task);
    }

    return tasks;
  }

  @Override
  public Iterable<BaseJob> getJobs() {
    LOG.info("Get all Jobs");
    File[] files = getFilesInDir(jobPath);

    List<BaseJob> jobs = new LinkedList<>();
    for (File file : files) {
      byte[] res;
      try {
        res = Files.readAllBytes(file.toPath());
        String jobDefinitionInJSON = new String(res);
        LOG.info("Def: " + jobDefinitionInJSON);
        jobs.add(BaseJob.getJobFromJSONString(jobDefinitionInJSON));
      } catch (IOException e) {
        throw new RuntimeException("Failed to read from file. ", e);
      }
    }

    return jobs;
  }

  @Override
  public Protos.FrameworkID getFrameworkID() {
    LOG.info("Get FrameworkID");

    if (Files.notExists(new File(frameworkIdPath).toPath())) {
      return null;
    }

    byte[] res;
    try {
      res = Files.readAllBytes(new File(frameworkIdPath).toPath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to read from file. ", e);
    }

    Protos.FrameworkID frameworkID =
        Protos.FrameworkID.newBuilder().setValue(new String(res)).build();

    return frameworkID;
  }

  @Override
  public boolean persistFrameworkID(Protos.FrameworkID frameworkID) {
    LOG.info("Persist FrameworkId: " + frameworkID);
    try {
      Files.write(
          new File(frameworkIdPath).toPath(), frameworkID.getValue().getBytes());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to write file", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean removeFrameworkID() {
    LOG.info("Remove frameworkId: " + frameworkIdPath);
    try {
      Files.deleteIfExists(new File(frameworkIdPath).toPath());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to remove frameworkID", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean clean() {
    LOG.info("Cleaning all unneeded meta-data");
    LOG.info("Doing some cleaning");

    try {
      Files.walkFileTree(new File(rootPath).toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }

      });
    } catch (Exception e) {
      LOG.info("Unable to clean");
      return false;
    }
    LOG.info("Clean done");

    return true;
  }

  public static File[] getFilesInDir(String path) {
    File dir = new File(path);
    return dir.listFiles();
  }
}
