/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

private[sql] object FileSystemHelper {

  def listFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.exists(path) || !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: path $path does not exist or is not a directory")
    }
    fs.listStatus(path).map(_.getPath)
  }

  /**
   *  List files with special extension
   */
  def listFilesEndsWith(origPath: Path, conf: Configuration, extension: String): Seq[Path] = {
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (fs.exists(path)) {
      fs.listStatus(path).map(_.getPath).filter(p => p.getName.endsWith(extension))
    } else {
      Seq.empty
    }
  }

  /**
   *  List files with special start
   */
  def listFiles1StartsWith(origPath: Path, conf: Configuration, start: String): Seq[Path] = {
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (fs.exists(path)) {
      fs.listStatus(path).map(_.getPath).filter(p => p.getName.startsWith(start))
    } else {
      Seq.empty
    }
  }

  /**
   * Finds the maximum taskid in the output file names at the given path.
   */
  def findMaxTaskId(pathStr: String, conf: Configuration, extension: String): Int = {
    // filename pattern is part-r-<int>.$extension
    require(Seq("orc", "parquet").contains(extension), s"Unsupported extension: $extension")
    val nameP = new scala.util.matching.Regex(s"""part-r-(\\d{1,}).$extension""", "taskid")
    val files = listFiles(pathStr, conf)
    val hiddenFileP = new scala.util.matching.Regex("_.*")
    files.map(_.getName).map {
      case nameP(taskid) => taskid.toInt
      case hiddenFileP() => 0
      case other: String =>
        sys.error(s"ERROR: attempting to append to set of $extension files and found file" +
          s"that does not match name pattern: $other")
      case _ => 0
    }.reduceLeft((a, b) => if (a < b) b else a)
  }
}
