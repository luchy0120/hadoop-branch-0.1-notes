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

package org.apache.hadoop.util;

import java.util.ArrayList;

/** Utility to assist with generation of progress reports.  Applications build
 * a hierarchy of {@link Progress} instances, each modelling a phase of
 * execution.  The root is constructed with {@link #Progress()}.  Nodes for
 * sub-phases are created by calling {@link #addPhase()}.
 */
public class Progress {
  private String status = "";
  private float progress;
  private int currentPhase;
  // 一个进度包含多个步骤
  private ArrayList phases = new ArrayList();
  private Progress parent;
  private float progressPerPhase;

  /** Creates a new root node. */
  public Progress() {}

 /** Adds a named node to the tree. */
  public Progress addPhase(String status) {
    Progress phase = addPhase();
    // 设置一下 status
    phase.setStatus(status);
    return phase;
  }

  /** Adds a node to the tree. */
  public Progress addPhase() {
    // 生成一个新的phase
    Progress phase = new Progress();
    // 添加儿子节点
    phases.add(phase);
    // 指向父亲节点
    phase.parent = this;
    // 每个步骤的占比
    progressPerPhase = 1.0f / (float)phases.size();
    return phase;
  }

  /** Called during execution to move to the next phase at this level in the
   * tree. */
  public void startNextPhase() {
    currentPhase++;
  }

  /** Returns the current sub-node executing. */
  public Progress phase() {
    return (Progress)phases.get(currentPhase);
  }

  /** Completes this node, moving the parent node to its next child. */
  public void complete() {
      // 当前的完成了
    progress = 1.0f;
    if (parent != null) {
        // 将parent的current指向下一个儿子
      parent.startNextPhase();
    }
  }

  /** Called during execution on a leaf node to set its progress. */
  // 设置progress
  public void set(float progress) {
    this.progress = progress;
  }

  /** Returns the overall progress of the root. */
  public float get() {
    Progress node = this;
    // 找到root
    while (node.parent != null) {                 // find the root
      node = parent;
    }
    return node.getInternal();
  }

  /** Computes progress in this node. */
  private float getInternal() {
      // 一共有几个步骤
    int phaseCount = phases.size();
    if (phaseCount != 0) {
        // 内部节点，返回这个内部节点的progress
      float subProgress =
        currentPhase < phaseCount ? phase().getInternal() : 0.0f;
         //  progressPerPhase是每个步骤的占比，当前的进度就是之前完成了多少个progressPerPhase
        // 加上现在完成的进度情况
      return progressPerPhase*(currentPhase + subProgress);
    } else {
        // 叶子节点直接返回progress
      return progress;
    }
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String toString() {
    StringBuffer result = new StringBuffer();
    toString(result);
    return result.toString();
  }

  private void toString(StringBuffer buffer) {
    buffer.append(status);
    if (phases.size() != 0 && currentPhase < phases.size()) {
      buffer.append(" > ");
      phase().toString(buffer);
    }
  }

}
