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


public class StressLimits
{
  public static final int MAX_SNAPSHOTS = 100;
  public static final int MIN_SNAPSHOTS = 40;
  public static final int MAX_FILES = 1048576;
  public static final int MIN_FILES = 1024;
  public static final int SNAPSHOTS_OP_PROBABILITY = 10;
  public static final int CREATE_FILE_PROBABILITY = 75;
  public static final int CHECKPOINT_INTERVAL_MS = 30000;
}
