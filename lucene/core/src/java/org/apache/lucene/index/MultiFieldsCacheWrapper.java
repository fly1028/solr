/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;

public final class MultiFieldsCacheWrapper extends MultiFields {
  /**
   * Expert: construct a new MultiFields instance directly.
   *
   * @param subs
   * @param subSlices
   * @lucene.internal
   */
  private MultiFieldsCacheWrapper(Fields[] subs, ReaderSlice[] subSlices) {
    super(subs, subSlices);
  }

  /** Returns a single {@link Fields} instance for this
   *  reader, merging fields/terms/docs/positions on the
   *  fly.  This method will return null if the reader
   *  has no postings.
   *
   *  <p><b>NOTE</b>: this is a slow way to access postings.
   *  It's better to get the sub-readers and iterate through them
   *  yourself. */
  private static MultiFields doGetFields(IndexReader reader) throws IOException {
    return doGetFields(reader, r -> true);
  }

  private static MultiFields doGetFields(IndexReader reader, Predicate<LeafReader> filter) throws IOException {
    final List<LeafReaderContext> leaves = reader.leaves();
    final List<Fields> fields = new ArrayList<>(leaves.size());
    final List<ReaderSlice> slices = new ArrayList<>(leaves.size());
    for (final LeafReaderContext ctx : leaves) {
      final LeafReader r = ctx.reader();
      if(!filter.test(r)){
        continue;
      }
      final Fields f = new LeafReaderFields(r);
      fields.add(f);
      slices.add(new ReaderSlice(ctx.docBase, r.maxDoc(), fields.size()-1));
    }
    return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
        slices.toArray(ReaderSlice.EMPTY_ARRAY));
  }

  private static MultiFields addSegmentFields(LeafReaderContext ctx, List<Fields> fields, List<ReaderSlice> slices){
    final LeafReader r = ctx.reader();
    final Fields f = new LeafReaderFields(r);
    fields.add(f);
    slices.add(new ReaderSlice(ctx.docBase, r.maxDoc(), fields.size()-1));
    return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
        slices.toArray(ReaderSlice.EMPTY_ARRAY));
  }

  /*

  Main Index: [seg1, seg2, seg3] -> Fields                                                                             1 entry per solr core
  Main Index + Commit Buffer: [seg1, seg2, seg3] + [segA, segB, segC] -> Fields                                        1 entry per solr core
  Main Index + Commit Buffer + Private Index: [seg1, seg2, seg3] + [segA, segB, segC] + [segX, segY, segZ] -> Fields   1 entry per TxID

  Given a composite IndexReader, we divide up the leave readers into 3 categories based on the directory of the segment readers

  First check the input composite reader to see if there's any private index reader (based on directory pattern), if there's any we will build a private index view based on the commit buffer view + main index view
  If there's no private index reader, we check to see if there's any commit buffer reader (again based on directory pattern), if there's any we will build a commit buffer view based on the main index view
  If there's only main index reader we will build a main index view

  For main index view: check to see if the main index segments in the input composite reader matches our cached Main Index segments, if more segments in the input, take the cached main index segments and add more segments to it. if less, rebuild main index segments cache.
  For commit buffer view: check to see if the commit buffer index segments in the input composite reader matches our cached Commit Buffer segments, if more segments in the input, take the cached commit buffer segments and add more segments to it. if less, rebuild commit buffer segments cache.
  For private index view: take the main index segment + commit buffer index segments and add private index segment to it and add to the private index segments cache.
   */
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, MultiFields>> mainSegmentFieldsCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, MultiFields>> commitBufferSegmentFieldsCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<String, Map<Set<IndexReader.CacheKey>, MultiFields>>> privateIndexSegmentFieldsCache = new ConcurrentHashMap<>();

  private static final Pattern PRIVATE_INDEX_DIR_PATTERN = Pattern.compile("[a-fA-F0-9]{32}");
  private static final Pattern COMMIT_BUFFER_DIR_PATTERN = Pattern.compile("COMMIT_BUFFER_[a-fA-F0-9]{32}");

  public static Fields getFields(IndexReader reader) throws IOException {
    final DirectoryReader directoryReader = (DirectoryReader) reader;
    final Directory mainDirectory = directoryReader.directory();
    final Map<String, Map<Set<IndexReader.CacheKey>, MultiFields>> privateSegmentFields = privateIndexSegmentFieldsCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, MultiFields> commitBufferSegmentFields = commitBufferSegmentFieldsCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, MultiFields> mainSegmentFields = mainSegmentFieldsCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());


    final Set<IndexReader.CacheKey> mainInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> commitBufferInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> privateIndexInputSegmentFields = new HashSet<>();
    final List<LeafReader> mainInputLeafReaders = new LinkedList<>();
    final List<LeafReader> commitBufferInputLeafReaders = new LinkedList<>();
    final List<LeafReader> privateIndexInputLeafReaders = new LinkedList<>();
    final Set<String> privateIndexSegmentPath = new HashSet<>();
    reader.leaves().forEach(ctx -> {
      LeafReader leafReader = FilterLeafReader.unwrap(ctx.reader());
      SegmentReader segmentReader = (SegmentReader) FilterCodecReader.unwrap((CodecReader) leafReader);
      Directory unwrappedDir = FilterDirectory.unwrap(segmentReader.directory());
      Path segmentPath = ((FSDirectory) unwrappedDir).getDirectory();
      IndexReader.CacheKey cacheKey = segmentReader.getReaderCacheHelper().getKey();
      if(COMMIT_BUFFER_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        commitBufferInputLeafReaders.add(ctx.reader());
        commitBufferInputSegmentFields.add(cacheKey);
      }else if(PRIVATE_INDEX_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        privateIndexInputLeafReaders.add(ctx.reader());
        privateIndexInputSegmentFields.add(cacheKey);
        privateIndexSegmentPath.add(segmentPath.toString());
      }else{
        mainInputLeafReaders.add(ctx.reader());
        mainInputSegmentFields.add(cacheKey);
      }
      //System.out.println("Segment: " + segmentPath + "/" + segmentReader.getSegmentName());
    });

//    System.out.println("privateSegmentFields: " + privateSegmentFields.size());
//    System.out.println("commitBufferSegmentFields: " + commitBufferSegmentFields.size());
//    System.out.println("mainSegmentFields: " + mainSegmentFields.size());

    if(privateIndexSegmentPath.size() > 1){
      throw new IllegalArgumentException("input reader contains segments across more than 1 transactional index (txId)");
    }

    final Set<IndexReader.CacheKey> commitBufferIndexKey = new HashSet<>();
    commitBufferIndexKey.addAll(mainInputSegmentFields);
    commitBufferIndexKey.addAll(commitBufferInputSegmentFields);

    if(!privateIndexInputLeafReaders.isEmpty()) {
      // This is a private index view with txId
      final Set<IndexReader.CacheKey> privateIndexKey = new HashSet<>();
      privateIndexKey.addAll(mainInputSegmentFields);
      privateIndexKey.addAll(commitBufferInputSegmentFields);
      privateIndexKey.addAll(privateIndexInputSegmentFields);

      String txId = privateIndexSegmentPath.iterator().next();
      Map<Set<IndexReader.CacheKey>, MultiFields> privateIndexFields = privateSegmentFields.computeIfAbsent(txId, d -> new ConcurrentHashMap<>());
      return computePrivateIndexMultiFields(reader, privateIndexKey, commitBufferIndexKey, mainInputSegmentFields, privateIndexInputLeafReaders, commitBufferInputLeafReaders, mainInputLeafReaders, privateIndexFields, commitBufferSegmentFields, mainSegmentFields, privateSegmentFields);
    }else if (!commitBufferInputLeafReaders.isEmpty()){
      // This is a commit buffer only view without txid
      return computeCommitBufferMultiFields(reader, commitBufferIndexKey, mainInputSegmentFields, commitBufferInputLeafReaders, mainInputLeafReaders, commitBufferSegmentFields, mainSegmentFields);
    }else{
      // This is a main index only view
      return computeMainIndexMultiFields(reader, mainInputSegmentFields, mainInputLeafReaders, mainSegmentFields);
    }
  }

  private static MultiFields computePrivateIndexMultiFields(IndexReader reader,
                                                                        Set<IndexReader.CacheKey> privateIndexKey,
                                                                        Set<IndexReader.CacheKey> commitBufferIndexKey,
                                                                        Set<IndexReader.CacheKey> mainInputSegmentFields,
                                                                        List<LeafReader> privateIndexInputLeafReaders,
                                                                        List<LeafReader> commitBufferInputLeafReaders,
                                                                        List<LeafReader> mainInputLeafReaders,
                                                                        Map<Set<IndexReader.CacheKey>, MultiFields> privateIndexFields,
                                                                        Map<Set<IndexReader.CacheKey>, MultiFields> commitBufferSegmentFields,
                                                                        Map<Set<IndexReader.CacheKey>, MultiFields> mainSegmentFields,
                                                                        Map<String, Map<Set<IndexReader.CacheKey>, MultiFields>> privateSegmentFields) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, MultiFields>> existingFields = privateIndexFields.entrySet().stream().findFirst();
    List<LeafReader> inputLeafReaders = privateIndexInputLeafReaders;

    if (existingFields.isPresent()) {
      Map.Entry<Set<IndexReader.CacheKey>, MultiFields> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(privateIndexKey)) {
        return existingEntry.getValue();
      }
    }

    MultiFields multiFields;
    synchronized (privateIndexFields) {
      // avoid the compute cost for concurrent threads to all get here and recompute the segment fields.
      // lookup from the cache again after we acquired the exclusive lock
      existingFields = privateIndexFields.entrySet().stream().findFirst();
      if (existingFields.isPresent() && existingFields.get().getKey().equals(privateIndexKey)) {
        return existingFields.get().getValue();
      }

      // not equal but current cache is a subset of new input, we can build from existing cache
      if (existingFields.isPresent() && privateIndexKey.containsAll(existingFields.get().getKey())) {
        final Map.Entry<Set<IndexReader.CacheKey>, MultiFields> finalExistingFields = existingFields.get();
        inputLeafReaders = privateIndexInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getReaderCacheHelper().getKey())).collect(Collectors.toList());
        multiFields = finalExistingFields.getValue();
      } else {
        multiFields = computeCommitBufferMultiFields(reader, commitBufferIndexKey, mainInputSegmentFields, commitBufferInputLeafReaders, mainInputLeafReaders, commitBufferSegmentFields, mainSegmentFields);
      }

      for (LeafReader privateIndexLeafReader : inputLeafReaders) {
        multiFields = addSegmentFields(privateIndexLeafReader.getContext(), new ArrayList<>(Arrays.asList(multiFields.subs)), new ArrayList<>(Arrays.asList(multiFields.subSlices)));
        // register on private index segment closed listener
        privateIndexLeafReader.getReaderCacheHelper().addClosedListener(closedCacheKey ->
            privateSegmentFields.entrySet().removeIf(entry -> entry.getValue().keySet().stream().anyMatch(keySet -> keySet.contains(closedCacheKey))));
      }

      privateIndexFields.clear();
      privateIndexFields.put(privateIndexKey, multiFields);
    }

    return multiFields;
  }

  private static MultiFields computeCommitBufferMultiFields(IndexReader reader,
                                                                        Set<IndexReader.CacheKey> commitBufferIndexKey,
                                                                        Set<IndexReader.CacheKey> mainInputSegmentFields,
                                                                        List<LeafReader> commitBufferInputLeafReaders,
                                                                        List<LeafReader> mainInputLeafReaders,
                                                                        Map<Set<IndexReader.CacheKey>, MultiFields> commitBufferSegmentFields,
                                                                        Map<Set<IndexReader.CacheKey>, MultiFields> mainSegmentFields) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, MultiFields>> existingFields = commitBufferSegmentFields.entrySet().stream().findFirst();

    if(existingFields.isPresent()){
      Map.Entry<Set<IndexReader.CacheKey>, MultiFields> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(commitBufferIndexKey)) {
        return existingEntry.getValue();
      }
    }

    MultiFields multiFields;
    synchronized (commitBufferSegmentFields) {
      // avoid the compute cost for concurrent threads to all get here and recompute the segment fields.
      // lookup from the cache again after we acquired the exclusive lock
      existingFields = commitBufferSegmentFields.entrySet().stream().findFirst();
      if (existingFields.isPresent() && existingFields.get().getKey().equals(commitBufferIndexKey)) {
        return existingFields.get().getValue();
      }

      // not equal but current cache is a subset of new input, we can build from existing cache
      if(existingFields.isPresent() && commitBufferIndexKey.containsAll(existingFields.get().getKey())){
        final Map.Entry<Set<IndexReader.CacheKey>, MultiFields> finalExistingFields = existingFields.get();
        commitBufferInputLeafReaders = commitBufferInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getReaderCacheHelper().getKey())).collect(Collectors.toList());
        multiFields = finalExistingFields.getValue();
      }else {
        multiFields = computeMainIndexMultiFields(reader, mainInputSegmentFields, mainInputLeafReaders, mainSegmentFields);
      }

      for (LeafReader commitBufferReader : commitBufferInputLeafReaders) {
        multiFields = addSegmentFields(commitBufferReader.getContext(), new ArrayList<>(Arrays.asList(multiFields.subs)), new ArrayList<>(Arrays.asList(multiFields.subSlices)));
      }
      commitBufferSegmentFields.clear();
      commitBufferSegmentFields.put(commitBufferIndexKey, multiFields);
    }

    return multiFields;
  }

  private static MultiFields computeMainIndexMultiFields(IndexReader reader,
                                                                     Set<IndexReader.CacheKey> mainInputSegmentFields,
                                                                     List<LeafReader> inputLeafReaders,
                                                                     Map<Set<IndexReader.CacheKey>, MultiFields> mainSegmentFields) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, MultiFields>> existingFields = mainSegmentFields.entrySet().stream().findFirst();

    if (existingFields.isPresent()) {
      Map.Entry<Set<IndexReader.CacheKey>, MultiFields> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(mainInputSegmentFields)) {
        return existingEntry.getValue();
      }
    }

    MultiFields multiFields;
    synchronized (mainSegmentFields) {
      // avoid the compute cost for concurrent threads to all get here and recompute the segment fields.
      // lookup from the cache again after we acquired the exclusive lock
      existingFields = mainSegmentFields.entrySet().stream().findFirst();
      if (existingFields.isPresent() && existingFields.get().getKey().equals(mainInputSegmentFields)) {
        return existingFields.get().getValue();
      }

      // not equal but current cache is a subset of new input, we can build from existing cache
      if (existingFields.isPresent() && mainInputSegmentFields.containsAll(existingFields.get().getKey())) {
        final Map.Entry<Set<IndexReader.CacheKey>, MultiFields> finalExistingFields = existingFields.get();
        inputLeafReaders = inputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getReaderCacheHelper().getKey())).collect(Collectors.toList());
        multiFields = finalExistingFields.getValue();

        for (LeafReader leafReader : inputLeafReaders) {
          multiFields = addSegmentFields(leafReader.getContext(), new ArrayList<>(Arrays.asList(multiFields.subs)), new ArrayList<>(Arrays.asList(multiFields.subSlices)));
        }

      } else {
        try {
          multiFields = doGetFields(reader, inputLeafReaders::contains);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      mainSegmentFields.clear();
      mainSegmentFields.put(mainInputSegmentFields, multiFields);
    }
    return multiFields;
  }


  private static FieldInfos.Builder addSegmentFieldInfos(LeafReaderContext ctx, FieldInfos.Builder fieldInfosBuilder){
    String newSoftDeletesField = ctx.reader().getFieldInfos().getSoftDeletesField();
    FieldInfos.Builder builder;

    if(newSoftDeletesField == null) {
      builder = fieldInfosBuilder;
    }else{
      // we can't reuse if newSoftDeletesField is not null
      builder = new FieldInfos.Builder(new FieldInfos.FieldNumbers(newSoftDeletesField));
      builder.add(fieldInfosBuilder.finish());
    }
    builder.add(ctx.reader().getFieldInfos());
    return builder;
  }


  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos>> mainSegmentFieldInfoCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos>> commitBufferFieldInfoCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<String, Map<Set<IndexReader.CacheKey>, FieldInfos>>> privateIndexFieldInfoCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> mainSegmentFieldInfoBuilderCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> commitBufferFieldInfoBuilderCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<String, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>>> privateIndexFieldInfoBuilderCache = new ConcurrentHashMap<>();

  public static FieldInfos getMergedFieldInfos(IndexReader reader){
    final DirectoryReader directoryReader = (DirectoryReader) reader;
    final Directory mainDirectory = directoryReader.directory();
    final Map<String, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> privateIndexSegmentFieldInfoBuilders = privateIndexFieldInfoBuilderCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> commitBufferSegmentFieldInfoBuilders = commitBufferFieldInfoBuilderCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> mainIndexSegmentFieldInfoBuilders = mainSegmentFieldInfoBuilderCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<String, Map<Set<IndexReader.CacheKey>, FieldInfos>> privateIndexSegmentFieldInfos = privateIndexFieldInfoCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos> commitBufferSegmentFieldInfos = commitBufferFieldInfoCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos> mainIndexSegmentFieldInfos = mainSegmentFieldInfoCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());


    final Set<IndexReader.CacheKey> mainInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> commitBufferInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> privateIndexInputSegmentFields = new HashSet<>();
    final List<LeafReader> mainInputLeafReaders = new LinkedList<>();
    final List<LeafReader> commitBufferInputLeafReaders = new LinkedList<>();
    final List<LeafReader> privateIndexInputLeafReaders = new LinkedList<>();
    final Set<String> privateIndexSegmentPath = new HashSet<>();
    reader.leaves().forEach(ctx -> {
      // TODO: .filter(r -> r instanceof CodecReader) and .filter(r -> r instanceof SegmentReader) ?
      LeafReader leafReader = FilterLeafReader.unwrap(ctx.reader());
      SegmentReader segmentReader = (SegmentReader) FilterCodecReader.unwrap((CodecReader) leafReader);
      Directory unwrappedDir = FilterDirectory.unwrap(segmentReader.directory());
      Path segmentPath = ((FSDirectory) unwrappedDir).getDirectory();
      IndexReader.CacheKey cacheKey = segmentReader.getReaderCacheHelper().getKey();
      if(COMMIT_BUFFER_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        commitBufferInputLeafReaders.add(ctx.reader());
        commitBufferInputSegmentFields.add(cacheKey);
      }else if(PRIVATE_INDEX_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        privateIndexInputLeafReaders.add(ctx.reader());
        privateIndexInputSegmentFields.add(cacheKey);
        privateIndexSegmentPath.add(segmentPath.toString());
      }else{
        mainInputLeafReaders.add(ctx.reader());
        mainInputSegmentFields.add(cacheKey);
      }
      //System.out.println("Segment: " + segmentPath + "/" + segmentReader.getSegmentName());
    });

//    System.out.println("privateIndexFieldInfoCache: " + privateIndexSegmentFieldInfoBuilders.size());
//    System.out.println("commitBufferSegmentFieldInfoBuilders: " + commitBufferSegmentFieldInfoBuilders.size());
//    System.out.println("mainIndexSegmentFieldInfoBuilders: " + mainIndexSegmentFieldInfoBuilders.size());

    if(privateIndexSegmentPath.size() > 1){
      throw new IllegalArgumentException("input reader contains segments across more than 1 transactional index (txId)");
    }

    final Set<IndexReader.CacheKey> commitBufferIndexKey = new HashSet<>();
    commitBufferIndexKey.addAll(mainInputSegmentFields);
    commitBufferIndexKey.addAll(commitBufferInputSegmentFields);

    FieldInfos.Builder result;
    if(!privateIndexInputLeafReaders.isEmpty()) {
      // This is a private index view with txId
      final Set<IndexReader.CacheKey> privateIndexKey = new HashSet<>();
      privateIndexKey.addAll(mainInputSegmentFields);
      privateIndexKey.addAll(commitBufferInputSegmentFields);
      privateIndexKey.addAll(privateIndexInputSegmentFields);

      String txId = privateIndexSegmentPath.iterator().next();
      Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> privateIndexSegmentFieldInfoBuilder = privateIndexSegmentFieldInfoBuilders.computeIfAbsent(txId, d -> new ConcurrentHashMap<>());
      Map<Set<IndexReader.CacheKey>, FieldInfos> privateIndexSegmentFieldInfo = privateIndexSegmentFieldInfos.computeIfAbsent(txId, d -> new ConcurrentHashMap<>());

      result = computePrivateIndexFieldInfos(reader,
          privateIndexKey, commitBufferIndexKey, mainInputSegmentFields,
          privateIndexInputLeafReaders, commitBufferInputLeafReaders, mainInputLeafReaders,
          privateIndexSegmentFieldInfoBuilder, commitBufferSegmentFieldInfoBuilders, mainIndexSegmentFieldInfoBuilders,
          privateIndexSegmentFieldInfo,  commitBufferSegmentFieldInfos, mainIndexSegmentFieldInfos,
          privateIndexSegmentFieldInfoBuilders, privateIndexSegmentFieldInfos);

      FieldInfos fieldInfos = privateIndexSegmentFieldInfo.get(commitBufferIndexKey);
      if(fieldInfos == null){
        synchronized (mainIndexSegmentFieldInfos) {
          return result.finish();
        }
      }else{
        return fieldInfos;
      }
    }else if (!commitBufferInputLeafReaders.isEmpty()){
      // This is a commit buffer only view without txid
      result = computeCommitBufferFieldInfosBuilder(reader, commitBufferIndexKey, mainInputSegmentFields, commitBufferInputLeafReaders, mainInputLeafReaders, commitBufferSegmentFieldInfoBuilders, mainIndexSegmentFieldInfoBuilders, commitBufferSegmentFieldInfos, mainIndexSegmentFieldInfos);
      FieldInfos fieldInfos = commitBufferSegmentFieldInfos.get(commitBufferIndexKey);
      if(fieldInfos == null){
        synchronized (commitBufferSegmentFieldInfos) {
          return result.finish();
        }
      }else{
        return fieldInfos;
      }
    }else{
      // This is a main index only view
      result = computeMainIndexFieldInfosBuilder(reader, mainInputSegmentFields, mainInputLeafReaders, mainIndexSegmentFieldInfoBuilders, mainIndexSegmentFieldInfos);
      FieldInfos fieldInfos = mainIndexSegmentFieldInfos.get(mainInputSegmentFields);
      if(fieldInfos == null){
        synchronized (mainIndexSegmentFieldInfos) {
          return result.finish();
        }
      }else{
        return fieldInfos;
      }
    }
  }

  private static FieldInfos.Builder computePrivateIndexFieldInfos(IndexReader reader,
                                                                  Set<IndexReader.CacheKey> privateIndexKey,
                                                                  Set<IndexReader.CacheKey> commitBufferIndexKey,
                                                                  Set<IndexReader.CacheKey> mainInputSegmentFields,
                                                                  List<LeafReader> privateIndexInputLeafReaders,
                                                                  List<LeafReader> commitBufferInputLeafReaders,
                                                                  List<LeafReader> mainInputLeafReaders,
                                                                  Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> privateIndexFieldBuilders,
                                                                  Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> commitBufferFieldBuilders,
                                                                  Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> mainSegmentFieldBuilders,
                                                                  Map<Set<IndexReader.CacheKey>, FieldInfos> privateIndexFieldsInfos,
                                                                  Map<Set<IndexReader.CacheKey>, FieldInfos> commitBufferFieldsInfos,
                                                                  Map<Set<IndexReader.CacheKey>, FieldInfos> mainFieldsInfos,
                                                                  Map<String, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> privateIndexSegmentFieldInfoBuilders,
                                                                  Map<String, Map<Set<IndexReader.CacheKey>, FieldInfos>> privateIndexSegmentFieldInfos) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder>> existingFields = privateIndexFieldBuilders.entrySet().stream().findFirst();
    List<LeafReader> inputLeafReaders = privateIndexInputLeafReaders;


    if (existingFields.isPresent()) {
      Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(privateIndexKey)) {
        return existingEntry.getValue();
      }
    }
    FieldInfos.Builder builder;

    synchronized (privateIndexFieldBuilders) {
      // avoid the compute cost for concurrent threads to all get here and recompute the segment fields.
      // lookup from the cache again after we acquired the exclusive lock
      existingFields = privateIndexFieldBuilders.entrySet().stream().findFirst();
      if (existingFields.isPresent() && existingFields.get().getKey().equals(privateIndexKey)) {
        return existingFields.get().getValue();
      }

      // not equal but current cache is a subset of new input, we can build from existing cache
      if (existingFields.isPresent() && privateIndexKey.containsAll(existingFields.get().getKey())) {
        final Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> finalExistingFields = existingFields.get();
        inputLeafReaders = privateIndexInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getReaderCacheHelper().getKey())).collect(Collectors.toList());
        builder = finalExistingFields.getValue();
      } else {
        builder = computeCommitBufferFieldInfosBuilder(reader,
            commitBufferIndexKey, mainInputSegmentFields,
            commitBufferInputLeafReaders, mainInputLeafReaders,
            commitBufferFieldBuilders, mainSegmentFieldBuilders,
            commitBufferFieldsInfos, mainFieldsInfos);
      }

      for (LeafReader privateIndexLeafReader : inputLeafReaders) {
        builder = addSegmentFieldInfos(privateIndexLeafReader.getContext(), builder);
        // register on private index segment closed listener
        privateIndexLeafReader.getReaderCacheHelper().addClosedListener(closedCacheKey -> {
          privateIndexSegmentFieldInfoBuilders.entrySet().removeIf(entry -> entry.getValue().keySet().stream().anyMatch(keySet -> keySet.contains(closedCacheKey)));
          privateIndexSegmentFieldInfos.entrySet().removeIf(entry -> entry.getValue().keySet().stream().anyMatch(keySet -> keySet.contains(closedCacheKey)));
        });
      }

      privateIndexFieldBuilders.clear();
      privateIndexFieldsInfos.clear();
      privateIndexFieldBuilders.put(privateIndexKey, builder);
      privateIndexFieldsInfos.put(privateIndexKey, builder.finish());
    }
    return builder;
  }


  private static FieldInfos.Builder computeCommitBufferFieldInfosBuilder(IndexReader reader,
                                                                         Set<IndexReader.CacheKey> commitBufferIndexKey,
                                                                         Set<IndexReader.CacheKey> mainInputSegmentFields,
                                                                         List<LeafReader> commitBufferInputLeafReaders,
                                                                         List<LeafReader> mainInputLeafReaders,
                                                                         Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> commitBufferSegmentFieldInfosBuilder,
                                                                         Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> mainSegmentFieldInfosBuilder,
                                                                         Map<Set<IndexReader.CacheKey>, FieldInfos> commitBufferSegmentFieldInfos,
                                                                         Map<Set<IndexReader.CacheKey>, FieldInfos> mainIndexSegmentFieldInfos) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder>> existingFields = commitBufferSegmentFieldInfosBuilder.entrySet().stream().findFirst();

    if(existingFields.isPresent()){
      Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(commitBufferIndexKey)) {
        return existingEntry.getValue();
      }
    }

    FieldInfos.Builder builder;
    synchronized (commitBufferSegmentFieldInfos) {
      // avoid the compute cost for concurrent threads to all get here and recompute the segment field infos.
      // lookup from the cache again after we acquired the exclusive lock
      existingFields = commitBufferSegmentFieldInfosBuilder.entrySet().stream().findFirst();
      if (existingFields.isPresent() && existingFields.get().getKey().equals(commitBufferIndexKey)) {
        return existingFields.get().getValue();
      }

      // not equal but current cache is a subset of new input, we can build from existing cache
      if (existingFields.isPresent() && commitBufferIndexKey.containsAll(existingFields.get().getKey())) {
        final Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> finalExistingFields = existingFields.get();
        commitBufferInputLeafReaders = commitBufferInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getReaderCacheHelper().getKey())).collect(Collectors.toList());
        builder = finalExistingFields.getValue();
      } else {
        builder = computeMainIndexFieldInfosBuilder(reader, mainInputSegmentFields, mainInputLeafReaders, mainSegmentFieldInfosBuilder, mainIndexSegmentFieldInfos);
      }

      for (LeafReader commitBufferReader : commitBufferInputLeafReaders) {
        builder = addSegmentFieldInfos(commitBufferReader.getContext(), builder);
      }
      commitBufferSegmentFieldInfosBuilder.clear();
      commitBufferSegmentFieldInfosBuilder.put(commitBufferIndexKey, builder);
      commitBufferSegmentFieldInfos.clear();
      commitBufferSegmentFieldInfos.put(commitBufferIndexKey, builder.finish());
    }

    return builder;
  }



  private static FieldInfos.Builder computeMainIndexFieldInfosBuilder(IndexReader reader,
                                                                      Set<IndexReader.CacheKey> mainInputSegmentFields,
                                                                      List<LeafReader> mainInputLeafReaders,
                                                                      Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> mainSegmentFieldInfosBuilder,
                                                                      Map<Set<IndexReader.CacheKey>, FieldInfos> mainIndexSegmentFieldInfos) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder>> existingFields = mainSegmentFieldInfosBuilder.entrySet().stream().findFirst();

    if (existingFields.isPresent()) {
      Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(mainInputSegmentFields)) {
        return existingEntry.getValue();
      }
    }

    FieldInfos.Builder builder;
    synchronized (mainIndexSegmentFieldInfos) {
      // avoid the compute cost for concurrent threads to all get here and recompute the segment fields.
      // lookup from the cache again after we acquired the exclusive lock
      existingFields = mainSegmentFieldInfosBuilder.entrySet().stream().findFirst();
      if (existingFields.isPresent() && existingFields.get().getKey().equals(mainInputSegmentFields)) {
        return existingFields.get().getValue();
      }

      // not equal but current cache is a subset of new input, we can build from existing cache
      if (existingFields.isPresent() && mainInputSegmentFields.containsAll(existingFields.get().getKey())) {
        final Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> finalExistingFields = existingFields.get();
        mainInputLeafReaders = mainInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getReaderCacheHelper().getKey())).collect(Collectors.toList());
        builder = finalExistingFields.getValue();

        for (LeafReader commitBufferReader : mainInputLeafReaders) {
          builder = addSegmentFieldInfos(commitBufferReader.getContext(), builder);
        }

      } else {
        builder = doGetMergedFieldInfos(reader, mainInputLeafReaders::contains);
      }
      mainSegmentFieldInfosBuilder.clear();
      mainIndexSegmentFieldInfos.clear();
      mainSegmentFieldInfosBuilder.put(mainInputSegmentFields, builder);
      mainIndexSegmentFieldInfos.put(mainInputSegmentFields, builder.finish());
    }
    return builder;
  }


  /** Call this to get the (merged) FieldInfos for a
   *  composite reader.
   *  <p>
   *  NOTE: the returned field numbers will likely not
   *  correspond to the actual field numbers in the underlying
   *  readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   *  will be unavailable.
   */
  private static FieldInfos.Builder doGetMergedFieldInfos(IndexReader reader, Predicate<LeafReader> filter) {
    final List<LeafReaderContext> leaves = reader.leaves();
    if (leaves.isEmpty()) {
      return new FieldInfos.Builder(new FieldInfos.FieldNumbers(null));
    } else {
      final String softDeletesField = leaves.stream()
          .map(l -> l.reader().getFieldInfos().getSoftDeletesField())
          .filter(Objects::nonNull)
          .findAny().orElse(null);
      final FieldInfos.Builder builder = new FieldInfos.Builder(new FieldInfos.FieldNumbers(softDeletesField));
      for (final LeafReaderContext ctx : leaves) {
        if(!filter.test(ctx.reader())){
          continue;
        }
        builder.add(ctx.reader().getFieldInfos());
      }
      return builder;
    }
  }

  private static FieldInfos.Builder doGetMergedFieldInfos(IndexReader reader) {
    return doGetMergedFieldInfos(reader, p -> true);
  }

}

