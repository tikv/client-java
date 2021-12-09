/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common.util;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientClassLoader {
  private static final Logger logger = LoggerFactory.getLogger(ClientClassLoader.class);
  /**
   * Get all classes under specific package root
   *
   * @param packageName package name
   * @return classes
   * @throws ClassNotFoundException class not found
   * @throws IOException file read failure
   */
  public static Class<?>[] getClasses(@Nonnull String packageName)
      throws ClassNotFoundException, IOException {
    logger.info("package name: " + packageName);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    assert classLoader != null;
    String path = packageName.replace('.', '/');
    logger.info("package path: " + path);
    Enumeration<URL> resources = classLoader.getResources(path);
    List<File> dirs = new ArrayList<>();
    ArrayList<Class<?>> classes = new ArrayList<>();
    while (resources.hasMoreElements()) {
      URL resource = resources.nextElement();
      String protocol = resource.getProtocol();
      if ("jar".equalsIgnoreCase(protocol)) {
        JarURLConnection connection = (JarURLConnection) resource.openConnection();
        classes.addAll(findClasses(connection, packageName));
      } else if ("file".equalsIgnoreCase(protocol)) {
        dirs.add(new File(resource.getFile()));
      }
    }
    for (File directory : dirs) {
      classes.addAll(findClasses(directory, packageName));
    }
    return classes.toArray(new Class[0]);
  }

  /**
   * Recursive method used to find all classes in a given directory and sub-dirs.
   *
   * @param directory The base directory
   * @param packageName The package name for classes found inside the base directory
   * @return The classes
   * @throws ClassNotFoundException class not found
   */
  @Nonnull
  private static List<Class<?>> findClasses(@Nonnull File directory, String packageName)
      throws ClassNotFoundException {
    List<Class<?>> classes = new ArrayList<>();
    if (!directory.exists()) {
      return classes;
    }
    File[] files = directory.listFiles();
    assert files != null;
    for (File file : files) {
      if (file.isDirectory()) {
        assert !file.getName().contains(".");
        classes.addAll(
            Objects.requireNonNull(findClasses(file, packageName + "." + file.getName())));
      } else if (file.getName().endsWith(".class")) {
        // load class
        classes.add(
            Class.forName(
                packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
      }
    }
    return classes;
  }

  private static List<Class<?>> findClasses(
      @Nonnull JarURLConnection connection, String packageName)
      throws ClassNotFoundException, IOException {
    List<Class<?>> classes = new ArrayList<>();
    JarFile jarFile = connection.getJarFile();
    if (jarFile != null) {
      Enumeration<JarEntry> jarEntryEnumeration = jarFile.entries();
      while (jarEntryEnumeration.hasMoreElements()) {
        JarEntry entry = jarEntryEnumeration.nextElement();
        String jarEntryName = entry.getName();
        if (jarEntryName.contains(".class")
            && jarEntryName.replaceAll("/", ".").startsWith(packageName)) {
          String className =
              jarEntryName.substring(0, jarEntryName.lastIndexOf(".")).replace("/", ".");
          Class<?> cls = Class.forName(className);
          classes.add(cls);
        }
      }
    }
    return classes;
  }
}
