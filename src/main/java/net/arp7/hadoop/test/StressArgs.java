/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

import org.apache.hadoop.fs.Path;

public class StressArgs {
  private Path testRoot;

  private StressArgs(String testRoot) {
    this.testRoot = new Path(testRoot);
  }

  static StressArgs parse(String[] args) {
    if (args.length != 1) {
      usage();
      System.exit(-1);
    }

    return new StressArgs(args[0]);
  }

  private static void usage() {
    System.out.println("Usage: SnapshotStress <root-directory>");
  }

  public Path getTestRoot() {
    return this.testRoot;
  }
}