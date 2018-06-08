/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with work for additional information
 * regarding copyright ownership.  The ASF licenses file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.arp7.hadoop.test;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import net.arp7.hadoop.test.FsState.Dir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simplistic stress test that attempts to create and delete many snapshots,
 * while creating/deleting/renaming files in between the snapshot operations.
 * 
 * The test will periodically save checkpoints and restart the NameNode.
 * 
 * Runs forever once invoked.
 */
public class SnapshotStress {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotStress.class);
  private final StressArgs testArgs;
  private final FileSystem fs;
  private final Configuration conf;
  private final Dir snapshotRoot;
  private final Dir outsideRoot;
  
  // A list of snapshots. The first entry in the list is the oldest snapshot.
  private List<String> snapshots;
  
  // Monotonically increasing counter used to generate snapshot names.
  private long snapshotIdCounter = 0L;

  // Monotonically increasing counter used to generate file names.
  private long fileIdCounter = 0L;

  // FileId on which the last rename/delete operation was performed.
  private long lastOpFileId = -1L;

  private final Random random;

  // Time until the next checkpoint operation in ms. Measured since
  // lastCheckpointOpTimeMs.
  private long nextCheckpointOpDelayMs;
  
  // Time at which the last checkpoint was taken. Retrieved using
  // {@link Time#monotonicNow}.
  private long lastCheckpointOpTimeMs;
  
  private final FsState fsState;

  public static void main(String[] args) throws Exception {
    StressArgs testArgs = StressArgs.parse(args);
    SnapshotStress test = new SnapshotStress(testArgs);
    test.run();
  }

  /**
   * Setup the test directories.
   *
   * @throws IOException
   */
  private void makeTestDirs() throws IOException {
    if ((fs.exists(testArgs.getTestRoot())) &&
        (fs.listStatus(testArgs.getTestRoot()).length > 0)) {
      throw new PathIOException(
          testArgs.getTestRoot().toString(),
          "Test Directory already exists and is non-empty.");
    }

    if (!fs.mkdirs(new Path(snapshotRoot.getFullPath())) ||
        !fs.mkdirs(new Path(outsideRoot.getFullPath()))) {
      throw new PathIOException(
          testArgs.getTestRoot().toString(),
          "Failed to create test directories.");
    }

    Shell.ShellCommandExecutor.execCommand(
        "hdfs", "dfsadmin", "-allowSnapshot", snapshotRoot.toString());

    LOG.info("Successfully created test directories under {}", testArgs
        .getTestRoot());
  }

  private SnapshotStress(StressArgs testArgs) throws IOException {
    this.testArgs = testArgs;
    conf = new HdfsConfiguration();
    fs = FileSystem.get(conf);

    if (!fs.getScheme().equalsIgnoreCase("hdfs")) {
      throw new IllegalArgumentException(
          "FileSystem must be hdfs." + fs.getScheme() +
              " is not supported.");
    }

    Path snapRoot = new Path(testArgs.getTestRoot(), "snapshotRoot");
    Path otherRoot = new Path(testArgs.getTestRoot(), "outsideDir");

    fsState = new FsState();
    snapshotRoot = fsState.mkdirs(snapRoot);
    outsideRoot = fsState.mkdirs(otherRoot);
    snapshots = new ArrayList<>();
    random = new Random();
  }

  private void run() throws IOException, InterruptedException {
    makeTestDirs();
    lastCheckpointOpTimeMs = Time.monotonicNow();
    scheduleNextCheckpointOperation();

    for (;;)
    {
      boolean doSnapshotOp = 
          random.nextInt(100) < StressLimits.SNAPSHOTS_OP_PROBABILITY;
      try {
        if (doSnapshotOp) {
          doSnapshotOperation();
        } else {
          doFileOperation();
        }

      } catch (FileIdNotFoundException e) {
        // We tried to do a delete/move before a file could be created
        // in the target directory. Just ignore and continue.
      } catch (ConnectException|SafeModeException e) {
        // Retry in a short while, assuming the NameNode will be up
        // soon.
        Thread.sleep(1000L);
      }

      if (isCheckpointDue()) {
        doCheckpointOperation();
        scheduleNextCheckpointOperation();
      }
    }
  }


  /**
   * Create a new NameNode checkpoint (FsImage) and restart the NameNode
   * so the checkpoint is reloaded.
   *
   * @throws IOException
   */
  private void doCheckpointOperation() throws IOException {
    Shell.ShellCommandExecutor.execCommand(
        "hdfs", "dfsadmin", "-safemode", "enter");

    Shell.ShellCommandExecutor.execCommand(
        "hdfs", "dfsadmin", "-saveNamespace");


    lastCheckpointOpTimeMs = Time.monotonicNow();
    LOG.info("Generated new FsImage");

    LOG.info("Stopping the NameNode");
    Shell.ShellCommandExecutor.execCommand(
        "hadoop-daemon.sh", "stop", "namenode");

    LOG.info("Restarting the NameNode");
    Shell.ShellCommandExecutor.execCommand(
        "hadoop-daemon.sh", "start", "namenode");
  }


  /**
   * Is it time to take a new checkpoint?
   * @return
   */
  private boolean isCheckpointDue() {
    long time = Time.monotonicNow();
    return time - lastCheckpointOpTimeMs >= nextCheckpointOpDelayMs;
  }


  /**
   * Schedule a new checkpoint to be taken sometime between
   * [CHECKPOINT_INTERVAL_MS, 2*CHECKPOINT_INTERVAL_MS) from now.
   */
  private void scheduleNextCheckpointOperation() {
    int jitter = random.nextInt(StressLimits.CHECKPOINT_INTERVAL_MS);
    nextCheckpointOpDelayMs =
        StressLimits.CHECKPOINT_INTERVAL_MS + jitter;
  }


  /**
   * Perform a file operation, which is one of:
   *     1. Create a file.
   *     2. Delete a file.
   *     3. Move a file out of the snapshot directory.
   *     4. Move a file into the snapshot directory.
   * @throws IOException
   * @throws SnapshotStress.FileIdNotFoundException
   */
  private void doFileOperation()
      throws IOException, SnapshotStress.FileIdNotFoundException {
    int totalFiles = filesInSnapshotDir.size() + filesOutside.size();

    if (totalFiles < StressLimits.MIN_FILES) {
      createFile();
    } else if (totalFiles >= StressLimits.MAX_FILES) {
      if (random.nextBoolean()) {
        deleteFile(filesInSnapshotDir, snapshotRoot);
      } else {
        deleteFile(filesOutside, outsideRoot);
      }
    }


    int coin = random.nextInt(100);

    if (coin < 40) {
      // Create a file with 40% probability.
      createFile();
    } else if (coin < 45) {
      // Delete a file from snapshot dir with 5% probability.
      deleteFile(filesInSnapshotDir, snapshotRoot);
    } else if (coin < 50) {
      // Delete a file out of snapshot dir with 5% probability.
      deleteFile(filesOutside, outsideRoot);
    } else if (coin < 75) {
      // Move a file out of snapshot dir with 25% probability.
      moveFile(filesInSnapshotDir, snapshotRoot, filesOutside, outsideRoot);
    } else {
      // Move a file into snapshot dir with 25% probability.
      moveFile(filesOutside, outsideRoot, filesInSnapshotDir, snapshotRoot);
    }
  }

  private void moveFile(TreeSet<Long> sourceSet, Path srcRoot,
                        TreeSet<Long> destSet, Path destRoot)
      throws IOException, SnapshotStress.FileIdNotFoundException
  {
    long id = pickTargetFile(sourceSet);
    Path sourcePath = getFilePath(id, srcRoot);
    Path destPath = getFilePath(id, destRoot);
    fs.rename(sourcePath, destPath);
    sourceSet.remove(id);
    destSet.add(id);
    LOG.info("Moved file {} -> {}", sourcePath, destPath);
  }

  private void deleteFile(TreeSet<Long> filesSet, Path root)
      throws IOException, FileIdNotFoundException
  {
    long id = pickTargetFile(filesSet);
    Path path = getFilePath(id, root);
    fs.delete(path, true);
    filesSet.remove(id);
    LOG.info("Deleted file {}", path);
  }

  private long pickTargetFile(TreeSet<Long> filesSet)
      throws FileIdNotFoundException {
    Long id = filesSet.ceiling(lastOpFileId);
    if (id == null) {
      id = filesSet.ceiling(lastOpFileId);
      if (id == null) {
        throw new FileIdNotFoundException();
      }
    }
    lastOpFileId = id;
    return id;
  }

  private void createFile() throws IOException {
    long id = ++fileIdCounter;
    Path path = getFilePath(id, snapshotRoot);
    fs.create(path, false).close();
    filesInSnapshotDir.add(id);
    LOG.info("Created file {}", path);
  }

  private void doSnapshotOperation() throws IOException {
    if (random.nextInt(100) < 95 &&
        snapshots.size() < StressLimits.MAX_SNAPSHOTS) {
      createSnapshot();
    } else {
      deleteSnapshots();
    }
  }

  private void deleteSnapshots() throws IOException {
    final int numSnapshotsToDelete = random.nextInt(snapshots.size());
    int i = 0;
    
    LOG.info("Deleting {} of {} snapshots",
        numSnapshotsToDelete, snapshots.size());
    while (i < numSnapshotsToDelete && snapshots.size() > 0) {
      final int deleteIndex = random.nextInt(snapshots.size());
      String name = snapshots.get(deleteIndex);
      fs.deleteSnapshot(snapshotRoot, name);
      snapshots.remove(deleteIndex);
      LOG.info("    >> Deleted snapshot {}", name);
      ++i;
    }
    LOG.info("Deleted {} snapshots.", i);
  }

  private void createSnapshot() throws IOException {
    String name = getNextSnapshotName();
    fs.createSnapshot(snapshotRoot, name);
    snapshots.add(name);
    LOG.info("Created snapshot {}", name);
  }

  private String getNextSnapshotName() {
    return String.format("snap%08d", ++snapshotIdCounter);
  }

  /**
   * Generate the path for a file, given its fileID.
   * 
   * Since we may be creating millions of files, we store them in
   * a 2-level directory structure so we don't have too many files
   * at the same directory level. This keeps some directory operations
   * efficient.
   *
   * @param id
   * @return
   */
  private Path getFilePath(long id, Path root) {
    String name = String.format("file%08d", id);
    String subdir = getSubdirForFile(id);
    return new Path(root.toString() + "/" + subdir + "/" + name);
  }


  private String getSubdirForFile(long id) {
    int d1 = (int)(id >> 16 & 0xFF);
    int d2 = (int)(id >> 8 & 0xFF);

    String dir1 = String.format("dir%03d", d1);
    String dir2 = String.format("dir%03d", d2);
    return dir1 + "/" + dir2;
  }

  private class FileIdNotFoundException extends Exception {
    private FileIdNotFoundException() {}
  }
}
