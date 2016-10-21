/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileIOChannelFactory}. */
@RunWith(JUnit4.class)
public class FileIOChannelFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private FileIOChannelFactory factory = new FileIOChannelFactory();

  private static KV<String, Long> mkFileSize(File f) {
    return KV.of(f.getPath(), f.length());
  }

  private void testCreate(Path path) throws Exception {
    String expected = "my test string";
    // First with the path string
    try (Writer writer = Channels.newWriter(
        factory.create(path.toString(), MimeTypes.TEXT), StandardCharsets.UTF_8.name())) {
      writer.write(expected);
    }
    assertThat(
        Files.readLines(path.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder(expected));

    // Delete the file before trying as URI
    assertTrue("Unable to delete file " + path, path.toFile().delete());

    // Second with the path URI
    try (Writer writer = Channels.newWriter(
        factory.create(path.toUri().toString(), MimeTypes.TEXT), StandardCharsets.UTF_8.name())) {
      writer.write(expected);
    }
    assertThat(
        Files.readLines(path.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder(expected));
  }

  @Test
  public void testCreateWithExistingFile() throws Exception {
    File existingFile = temporaryFolder.newFile();
    testCreate(existingFile.toPath());
  }

  @Test
  public void testCreateWithinExistingDirectory() throws Exception {
    testCreate(temporaryFolder.getRoot().toPath().resolve("file.txt"));
  }

  @Test
  public void testCreateWithNonExistentSubDirectory() throws Exception {
    testCreate(temporaryFolder.getRoot().toPath().resolve("non-existent-dir").resolve("file.txt"));
  }

  @Test
  public void testReadWithExistingFile() throws Exception {
    String expected = "my test string";
    File existingFile = temporaryFolder.newFile();
    Files.write(expected, existingFile, StandardCharsets.UTF_8);
    String data;
    try (Reader reader =
        Channels.newReader(factory.open(existingFile.getPath()), StandardCharsets.UTF_8.name())) {
      data = new LineReader(reader).readLine();
    }
    assertEquals(expected, data);
  }

  @Test
  public void testReadNonExistentFile() throws Exception {
    thrown.expect(FileNotFoundException.class);
    factory
        .open(
            temporaryFolder
                .getRoot()
                .toPath()
                .resolve("non-existent-file.txt")
                .toString())
        .close();
  }

  @Test
  public void testIsReadSeekEfficient() throws Exception {
    assertTrue(factory.isReadSeekEfficient("somePath"));
  }

  @Test
  public void testMatchExact() throws Exception {
    File tempFile = temporaryFolder.newFile("a");
    temporaryFolder.newFile("aa");
    temporaryFolder.newFile("ab");

    Collection<KV<String, Long>> expected =
        Collections.singletonList(KV.of(tempFile.toString(), 0L));
    assertThat(
        factory.match(temporaryFolder.getRoot().toPath().resolve("a").toString()),
        equalTo(expected));
  }

  @Test
  public void testMatchNone() throws Exception {
    temporaryFolder.newFile("a");
    temporaryFolder.newFile("aa");
    temporaryFolder.newFile("ab");

    // Windows doesn't like resolving paths with * in them, so the * is appended after resolve.
    assertThat(
        factory.match(factory.resolve(temporaryFolder.getRoot().getPath(), "b") + "*"),
        empty());
  }

  @Test
  public void testMatchUsingExplicitPath() throws Exception {
    File tempFile = temporaryFolder.newFile("a");
    temporaryFolder.newFile("aa");

    Collection<KV<String, Long>> expected =
        Collections.singletonList(KV.of(tempFile.toString(), 0L));
    assertThat(
        factory.match(factory.resolve(temporaryFolder.getRoot().getPath(), "a")),
        equalTo(expected));
  }

  @Test
  public void testMatchUsingExplicitPathForNonExistentFile() throws Exception {
    temporaryFolder.newFile("aa");

    assertThat(
        factory.match(factory.resolve(temporaryFolder.getRoot().getPath(), "a")),
        empty());
  }

  @Test
  public void testMatchMultipleWithoutSubdirectoryExpansion() throws Exception {
    File unmatchedSubDir = temporaryFolder.newFolder("aaa");
    File unmatchedSubDirFile = File.createTempFile("sub-dir-file", "", unmatchedSubDir);
    unmatchedSubDirFile.deleteOnExit();


    List<KV<String, Long>> expected = ImmutableList.of(
        mkFileSize(temporaryFolder.newFile("a")),
        mkFileSize(temporaryFolder.newFile("aa")),
        mkFileSize(temporaryFolder.newFile("ab")));
    temporaryFolder.newFile("ba");
    temporaryFolder.newFile("bb");

    // Windows doesn't like resolving paths with * in them, so the * is appended after resolve.
    assertThat(
        factory.match(factory.resolve(temporaryFolder.getRoot().getPath(), "a") + "*"),
        containsInAnyOrder(expected.toArray(new KV[expected.size()])));
  }

  @Test
  public void testMatchMultipleWithSubdirectoryExpansion() throws Exception {
    File matchedSubDir = temporaryFolder.newFolder("a");
    File matchedSubDirFile = File.createTempFile("sub-dir-file", "", matchedSubDir);
    matchedSubDirFile.deleteOnExit();
    File unmatchedSubDir = temporaryFolder.newFolder("b");
    File unmatchedSubDirFile = File.createTempFile("sub-dir-file", "", unmatchedSubDir);
    unmatchedSubDirFile.deleteOnExit();

    List<KV<String, Long>> expected = ImmutableList.of(
        mkFileSize(matchedSubDirFile),
        mkFileSize(temporaryFolder.newFile("aa")),
        mkFileSize(temporaryFolder.newFile("ab")));
    temporaryFolder.newFile("ba");
    temporaryFolder.newFile("bb");

    // Windows doesn't like resolving paths with * in them, so the ** is appended after resolve.
    assertThat(
        factory.match(factory.resolve(temporaryFolder.getRoot().getPath(), "a") + "**"),
        containsInAnyOrder(expected.toArray(new KV[expected.size()])));
  }

  @Test
  public void testMatchWithDirectoryFiltersOutDirectory() throws Exception {
    List<KV<String, Long>> expected = ImmutableList.of(mkFileSize(temporaryFolder.newFile("a")));
    temporaryFolder.newFolder("a_dir_that_should_not_be_matched");

    // Windows doesn't like resolving paths with * in them, so the * is appended after resolve.
    assertThat(factory.match(factory.resolve(temporaryFolder.getRoot().getPath(), "a") + "*"),
        containsInAnyOrder(expected.toArray(new KV[expected.size()])));
  }

  @Test
  public void testResolve() throws Exception {
    Path rootPath = temporaryFolder.getRoot().toPath();
    String rootString = rootPath.toString();

    String expected = rootPath.resolve("aa").toString();
    assertEquals(expected, factory.resolve(rootString, "aa"));
    assertEquals(expected, factory.resolve("file:" + rootString, "aa"));
    assertEquals(expected, factory.resolve("file://" + rootString, "aa"));
  }

  @Test
  public void testResolveOtherIsFullPath() throws Exception {
    String expected = temporaryFolder.getRoot().getPath();
    assertEquals(expected, factory.resolve(expected, expected));
  }

  @Test
  public void testResolveOtherIsEmptyPath() throws Exception {
    String expected = temporaryFolder.getRoot().getPath();
    assertEquals(expected, factory.resolve(expected, ""));
  }

  @Test
  public void testGetSizeBytes() throws Exception {
    String data = "TestData!!!";
    File file = temporaryFolder.newFile();
    Files.write(data, file, StandardCharsets.UTF_8);
    assertEquals(data.length(), factory.getSizeBytes(file.getPath()));
  }

  @Test
  public void testGetSizeBytesForNonExistentFile() throws Exception {
    thrown.expect(FileNotFoundException.class);
    factory.getSizeBytes(
        factory.resolve(temporaryFolder.getRoot().getPath(), "non-existent-file"));
  }
}
