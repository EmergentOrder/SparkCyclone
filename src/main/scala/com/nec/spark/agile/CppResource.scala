/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
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
 *
 */
package com.nec.spark.agile
import com.nec.spark.agile.CppResource.CppPrefixPath

import java.nio.file.Files
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException
import org.reflections.scanners.ResourcesScanner

import java.net.URL
import java.nio.file.Path
import java.util.regex.Pattern

object CppResource {
  val CppPrefix = "com.nec.cyclone.cpp"
  val CppPrefixPath: String = CppPrefix.replace('.', '/')

  final case class CppResources(all: Set[CppResource]) {
    def copyTo(destRoot: Path): Unit = {
      all.foreach(_.copyTo(destRoot))
    }
  }

  object CppResources {
    lazy val All: CppResources = CppResources({
      import org.reflections.Reflections
      val reflections = new Reflections(CppPrefix, new ResourcesScanner)
      import scala.collection.JavaConverters._
      reflections
        .getResources(Pattern.compile(".*"))
        .asScala
        .toList
        .map(nom => {
          val nam = nom.drop(CppPrefix.length).drop(1)
          CppResource(nam, s"/$nom")
        })
        .toSet
    })
    lazy val cycloneVeResources = {
      import org.reflections.Reflections
      val reflections = new Reflections("cycloneve", new ResourcesScanner)
      import scala.collection.JavaConverters._
      reflections
        .getResources(Pattern.compile(".*\\.so"))
        .asScala
        .toList
        .collect {
          case name if name.contains(".so") =>
            CppResource(name = name.drop("cycloneve".length + 1), fullPath = s"/$name")
        }
        .toSet
    }
    lazy val AllVe: CppResources = CppResources(cycloneVeResources ++ All.all)
  }

}

final case class CppResource(name: String, fullPath: String) {
  def readString: String = IOUtils.toString(resourceUrl.openStream(), "UTF-8")
  def resourceUrl: URL = {
    try this.getClass.getResource(fullPath)
    catch {
      case npe: NullPointerException =>
        throw new ResourceNotFoundException(s"Not found: ${name} // '${fullPath}'")
    }
  }
  def resourceFile(inRoot: Path): Path = inRoot.resolve(name)
  def containingDir(inRoot: Path): Path = resourceFile(inRoot).getParent
  def copyTo(destRoot: Path): Unit = {
    val targetFile = resourceFile(destRoot)
    if (!Files.exists(targetFile.getParent)) {
      Files.createDirectories(targetFile.getParent)
    }
    FileUtils.copyURLToFile(resourceUrl, targetFile.toFile)
  }
}
