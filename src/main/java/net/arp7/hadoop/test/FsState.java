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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;


/**
 * A Class that can maintain an in-memory snapshot of a filesystem
 * structure, and support simple operations like create, delete and
 * rename.
 */
public class FsState {
  public static final Logger LOG = LoggerFactory.getLogger(FsState.class);
  
  public static final String SEPARATOR = "/";

  private final Dir root;

  public FsState() {
    this.root = Dir.newRootDir();
  }

  /**
   * A class representing a node. Could be a file or a directory.
   */
  public static class Node {
    public Node(String name, Node parent) {
      this.name = name;
      this.parent = parent;
    }

    protected String name;
    protected Node parent;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Node)) return false;
      Node node = (Node) o;
      return Objects.equals(name, node.name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
    
    public String getFullPath() {
      return parent.getFullPath() + SEPARATOR + name;
    }
  }

  /**
   * A class representing a directory.
   */
  public static class Dir extends Node {
    public Dir(String name, Node parent) {
      super(name, parent);
      this.children = new TreeSet<>();
    }

    private NavigableSet<Node> children;
    
    
    public static Dir newRootDir() {
      return new Dir(SEPARATOR, null);
    }
    
    public boolean isRootDir() {
      return parent == null;
    }    
    
    public Node addFile(String fileName) throws IOException {
      final Node child = new Node(fileName, this);
      if (children.contains(child)) {
        throw new FileAlreadyExistsException(
            "Path already exists " + getFullPath() + SEPARATOR + child.name);
      }
      
      children.add(child);
      child.parent = this;
      return child;
    }
    
    public Dir addDir(String dirName) throws IOException {
      final Dir child = new Dir(dirName, this);
      if (children.contains(child)) {
        throw new FileAlreadyExistsException(
            "Path already exists " + getFullPath() + SEPARATOR + child.name);
      }

      children.add(child);
      child.parent = this;
      return child;      
    }
    
    public void deleteChild(Node child) throws IOException {
      if (!children.contains(child)) {
        throw new FileNotFoundException(
            "Path not found " + getFullPath() + SEPARATOR + child.name);
      }
      
      children.remove(child);
      child.parent = null;
    }
    
    @Override
    public String getFullPath() {
      return isRootDir() ? SEPARATOR : parent.getFullPath() + SEPARATOR + name;
    }
  }

  /**
   * Create an absolute path.
   * @param otherRoot
   * @return
   */
  public Dir mkdirs(Path path) throws IOException {
    
    if (!path.toString().startsWith("/")) {
      throw new IllegalArgumentException("Path must be absolute " + path);
    }

    // Split the path into its components.
    final String[] components = path.toString().split("/");
    
    if (components.length == 0) {
      throw new IllegalArgumentException("Bad path " + path);
    }

    Dir parent = root;
    Dir child = null;
    
    
    for (final String c: components) {
      child = parent.addDir(c);
      parent = child;
    }
    
    return child;
  }
}
