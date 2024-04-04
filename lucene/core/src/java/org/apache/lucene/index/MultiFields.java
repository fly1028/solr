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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MergedIterator;

/**
 * Provides a single {@link Fields} term index view over an
 * {@link IndexReader}.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg {@link DirectoryReader} or {@link
 * MultiReader}) and you must treat it as a {@link LeafReader}.
 *
 * <p><b>NOTE</b>: for composite readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-LeafReader,
 * instead of using this class.
 *
 * @lucene.experimental
 */
public final class MultiFields extends Fields {
  private final Fields[] subs;
  private final ReaderSlice[] subSlices;
  private final Map<String,Terms> terms = new ConcurrentHashMap<>();
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
    final Fields f = new MultiFields.LeafReaderFields(r);
    fields.add(f);
    slices.add(new ReaderSlice(ctx.docBase, r.maxDoc(), fields.size()-1));
    return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
        slices.toArray(ReaderSlice.EMPTY_ARRAY));
  }

  /**
   * Finds the entry with the key representing the largest non-empty subset of the input set among the provided map entries.
   *
   * @param map       The map containing key-value entries.
   * @param inputSet  The input set for which the largest non-empty subset key is to be found.
   * @param <K>       The type of elements in the sets.
   * @param <V>       The type of values in the map.
   * @return          The entry containing the key representing the largest non-empty subset of the input set and its corresponding value,
   *                  or null if no such entry is found.
   */
  private static <K, V> Map.Entry<Set<K>, V> findEntryWithLargestSubsetKey(Map<Set<K>, V> map, Set<K> inputSet) {
    return map.entrySet().stream()
        .filter(entry -> !entry.getKey().isEmpty()) // Filter out empty subsets
        .filter(entry -> inputSet.containsAll(entry.getKey()))
        .max(Comparator.comparingInt(entry -> entry.getKey().size()))
        .orElse(null);
  }

  /*

  Main Index: [seg1, seg2, seg3] -> Fields                                                                             1 entry per solr core
  Main Index + Commit Buffer: [seg1, seg2, seg3] + [segA, segB, segC] -> Fields                                        1 entry per solr core
  Main Index + Commit Buffer + Private Index: [seg1, seg2, seg3] + [segA, segB, segC] + [segX, segY, segZ] -> Fields   1 entry per TxID

  Given a composite IndexReader, we divide up the leave readers into 3 categories based on the directory of the segment readers

  First check the input composite reader to see if there's any private index reader (based on directory pattern), if there's any we will build a private index view based on the commit buffer view + main index view
  If there's no private index reader, we check to see if there's any commit buffer reader (again based on directory pattern), if there's any we will build a commit buffer view based on the main index view
  If there's only main index reader we will build a main index view

  For main index view: check to see if the main index segments in the input composite reader matches our cached Main Index segments, if there's no exact match, always rebuild main index segments cache (to be optimized if needed but main index only view is rare)
  Second, check to see if the commit buffer index segments in the input composite reader matches our cached Commit Buffer segments, if more segments in the input, take the cached commit buffer segments and add more segments to it. if less, rebuild commit buffer segments cache.
  Last, take the main index segment + commit buffer index segments and add private index segment to it and add to the private index segments cache.

  lookup from shardPrivateIndexSegmentFields, if no exact match, lookup main + commit buffer cacheKey from shardCommitBufferSegmentFields, if no exact match, lookup main cacheKey from shardMainSegmentFields
   */
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, MultiFields>> mainSegmentFieldsCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, MultiFields>> commitBufferSegmentFieldsCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, MultiFields>> privateIndexSegmentFieldsCache = new ConcurrentHashMap<>();

  private static final Pattern PRIVATE_INDEX_DIR_PATTERN = Pattern.compile("[a-fA-F0-9]{32}");
  private static final Pattern COMMIT_BUFFER_DIR_PATTERN = Pattern.compile("COMMIT_BUFFER_[a-fA-F0-9]{32}");

  public static Fields getFields(IndexReader reader) throws IOException {

    if(!(reader instanceof DirectoryReader)){
      return doGetFields(reader);
    }

    final DirectoryReader directoryReader = (DirectoryReader) reader;
    final Directory mainDirectory = directoryReader.directory();
    final Map<Set<IndexReader.CacheKey>, MultiFields> privateSegmentFields = privateIndexSegmentFieldsCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, MultiFields> commitBufferSegmentFields = commitBufferSegmentFieldsCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, MultiFields> mainSegmentFields = mainSegmentFieldsCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());


    final Set<IndexReader.CacheKey> mainInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> commitBufferInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> privateIndexInputSegmentFields = new HashSet<>();
    final List<LeafReader> mainInputLeafReaders = new LinkedList<>();
    final List<LeafReader> commitBufferInputLeafReaders = new LinkedList<>();
    final List<LeafReader> privateIndexInputLeafReaders = new LinkedList<>();

    reader.leaves().forEach(ctx -> {
      // TODO: .filter(r -> r instanceof CodecReader) and .filter(r -> r instanceof SegmentReader) ?
      LeafReader leafReader = FilterLeafReader.unwrap(ctx.reader());
      SegmentReader segmentReader = (SegmentReader) FilterCodecReader.unwrap((CodecReader) leafReader);
      Directory unwrappedDir = FilterDirectory.unwrap(segmentReader.directory());
      Path segmentPath = ((FSDirectory) unwrappedDir).getDirectory();
      IndexReader.CacheKey cacheKey = segmentReader.getCoreCacheHelper().getKey();
      if(COMMIT_BUFFER_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        commitBufferInputLeafReaders.add(ctx.reader());
        commitBufferInputSegmentFields.add(cacheKey);
      }else if(PRIVATE_INDEX_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        privateIndexInputLeafReaders.add(ctx.reader());
        privateIndexInputSegmentFields.add(cacheKey);
      }else{
        mainInputLeafReaders.add(ctx.reader());
        mainInputSegmentFields.add(cacheKey);
      }
      //System.out.println("Segment: " + segmentPath + "/" + segmentReader.getSegmentName());
    });

//    System.out.println("privateSegmentFields: " + privateSegmentFields.size());
//    System.out.println("commitBufferSegmentFields: " + commitBufferSegmentFields.size());
//    System.out.println("mainSegmentFields: " + mainSegmentFields.size());

    final Set<IndexReader.CacheKey> commitBufferIndexKey = new HashSet<>();
    commitBufferIndexKey.addAll(mainInputSegmentFields);
    commitBufferIndexKey.addAll(commitBufferInputSegmentFields);

    // This is a private index view with txId
    if(!privateIndexInputLeafReaders.isEmpty()) {
      final Set<IndexReader.CacheKey> privateIndexKey = new HashSet<>();
      privateIndexKey.addAll(mainInputSegmentFields);
      privateIndexKey.addAll(commitBufferInputSegmentFields);
      privateIndexKey.addAll(privateIndexInputSegmentFields);
      // We can have 1 cache entry per txId
      return privateSegmentFields.computeIfAbsent(privateIndexKey, key -> {
        MultiFields multiFields = computeCommitBufferMultiFields(reader,  mainInputSegmentFields, mainSegmentFields, mainInputLeafReaders, commitBufferIndexKey, commitBufferSegmentFields, commitBufferInputLeafReaders);;
        // Build private index multi fields from commitBufferMultiFields and store into cache
        for (LeafReader commitBufferReader : privateIndexInputLeafReaders) {
          multiFields = addSegmentFields(commitBufferReader.getContext(), new ArrayList<>(Arrays.asList(multiFields.subs)), new ArrayList<>(Arrays.asList(multiFields.subSlices)));
          // register on private index segment closed listener
          commitBufferReader.getCoreCacheHelper().addClosedListener(closedCacheKey ->
              privateSegmentFields.entrySet().removeIf(entry -> entry.getKey().contains(closedCacheKey)));
        }

        return multiFields;
      });
    // This is a commit buffer only view without txid
    }else if (!commitBufferInputLeafReaders.isEmpty()){
      return computeCommitBufferMultiFields(reader, mainInputSegmentFields, mainSegmentFields, mainInputLeafReaders, commitBufferIndexKey, commitBufferSegmentFields, commitBufferInputLeafReaders);
   // This is a main index only view
    }else{
      return computeMainIndexMultiFields(reader, mainInputSegmentFields, mainSegmentFields, mainInputLeafReaders);
    }
  }

  private static MultiFields computeMainIndexMultiFields(IndexReader reader, Set<IndexReader.CacheKey> inputSegmentFields, Map<Set<IndexReader.CacheKey>, MultiFields> segmentFieldsCache, List<LeafReader> inputLeafReaders) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, MultiFields>> existingFields = segmentFieldsCache.entrySet().stream().findFirst();
    MultiFields multiFields = null;

    if (existingFields.isPresent()) {
      Map.Entry<Set<IndexReader.CacheKey>, MultiFields> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(inputSegmentFields)) {
        multiFields = existingEntry.getValue();
      } else {
        segmentFieldsCache.remove(existingEntry.getKey());
      }
    }

    if (multiFields == null) {
      try {
        multiFields = doGetFields(reader, inputLeafReaders::contains);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      segmentFieldsCache.put(inputSegmentFields, multiFields);
    }

    return multiFields;
  }

  private static FieldInfos.Builder computeMainIndexFieldInfosBuilder(IndexReader reader, Set<IndexReader.CacheKey> inputSegmentFields, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> mainSegmentFieldInfosBuilder, List<LeafReader> inputLeafReaders, Map<Set<IndexReader.CacheKey>, FieldInfos> mainIndexSegmentFieldInfos) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder>> existingFields = mainSegmentFieldInfosBuilder.entrySet().stream().findFirst();
    FieldInfos.Builder builder = null;

    if (existingFields.isPresent()) {
      Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(inputSegmentFields)) {
        builder = existingEntry.getValue();
      } else {
        mainSegmentFieldInfosBuilder.remove(existingEntry.getKey());
        mainIndexSegmentFieldInfos.remove(existingEntry.getKey());
      }
    }

    if (builder == null) {
      builder = doGetMergedFieldInfos(reader, inputLeafReaders::contains);
      mainSegmentFieldInfosBuilder.put(inputSegmentFields, builder);
      mainIndexSegmentFieldInfos.put(inputSegmentFields, builder.finish());
    }

    return builder;
  }


  private static MultiFields computeCommitBufferMultiFields(IndexReader reader, Set<IndexReader.CacheKey> mainInputSegmentFields, Map<Set<IndexReader.CacheKey>, MultiFields> mainSegmentFields, List<LeafReader> mainInputLeafReaders, Set<IndexReader.CacheKey> commitBufferIndexKey, Map<Set<IndexReader.CacheKey>, MultiFields> commitBufferSegmentFields, List<LeafReader> commitBufferInputLeafReaders) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, MultiFields>> existingFields = commitBufferSegmentFields.entrySet().stream().findFirst();
    MultiFields multiFields = null;

    if(existingFields.isPresent()){
      Map.Entry<Set<IndexReader.CacheKey>, MultiFields> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(commitBufferIndexKey)) {
        multiFields = existingEntry.getValue();
      }
    }

    if(multiFields == null) {
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
          commitBufferInputLeafReaders = commitBufferInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getCoreCacheHelper().getKey())).collect(Collectors.toList());
          multiFields = finalExistingFields.getValue();
        }else {
          multiFields = computeMainIndexMultiFields(reader, mainInputSegmentFields, mainSegmentFields, mainInputLeafReaders);
        }

        for (LeafReader commitBufferReader : commitBufferInputLeafReaders) {
          multiFields = addSegmentFields(commitBufferReader.getContext(), new ArrayList<>(Arrays.asList(multiFields.subs)), new ArrayList<>(Arrays.asList(multiFields.subSlices)));
        }
        commitBufferSegmentFields.clear();
        commitBufferSegmentFields.put(commitBufferIndexKey, multiFields);
      }
    }
    return multiFields;
  }


  private static FieldInfos.Builder computeCommitBufferFieldInfosBuilder(IndexReader reader, Set<IndexReader.CacheKey> mainInputSegmentFields, Map<Set<IndexReader.CacheKey>,
      FieldInfos.Builder> mainSegmentFieldInfosBuilder, List<LeafReader> mainInputLeafReaders, Set<IndexReader.CacheKey> commitBufferIndexKey, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> commitBufferSegmentFieldInfosBuilder, List<LeafReader> commitBufferInputLeafReaders,
                                                                         Map<Set<IndexReader.CacheKey>, FieldInfos> mainIndexSegmentFieldInfos, Map<Set<IndexReader.CacheKey>, FieldInfos> commitBufferSegmentFieldInfos) {
    Optional<Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder>> existingFields = commitBufferSegmentFieldInfosBuilder.entrySet().stream().findFirst();
    FieldInfos.Builder builder = null;

    if(existingFields.isPresent()){
      Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> existingEntry = existingFields.get();
      if (existingEntry.getKey().equals(commitBufferIndexKey)) {
        builder = existingEntry.getValue();
      }
    }

    if(builder == null) {
      synchronized (commitBufferSegmentFieldInfos) {
        // avoid the compute cost for concurrent threads to all get here and recompute the segment field infos.
        // lookup from the cache again after we acquired the exclusive lock
        existingFields = commitBufferSegmentFieldInfosBuilder.entrySet().stream().findFirst();
        if(existingFields.isPresent() && existingFields.get().getKey().equals(commitBufferIndexKey)){
          return existingFields.get().getValue();
        }

        // not equal but current cache is a subset of new input, we can build from existing cache
        if(existingFields.isPresent() && commitBufferIndexKey.containsAll(existingFields.get().getKey())){
          final Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> finalExistingFields = existingFields.get();
          commitBufferInputLeafReaders = commitBufferInputLeafReaders.stream().filter(r -> !finalExistingFields.getKey().contains(r.getCoreCacheHelper().getKey())).collect(Collectors.toList());
          builder = finalExistingFields.getValue();
        }else {
          builder = computeMainIndexFieldInfosBuilder(reader, mainInputSegmentFields, mainSegmentFieldInfosBuilder, mainInputLeafReaders, mainIndexSegmentFieldInfos);
        }

        for (LeafReader commitBufferReader : commitBufferInputLeafReaders) {
          builder = addSegmentFieldInfos(commitBufferReader.getContext(), builder);
        }
        commitBufferSegmentFieldInfosBuilder.clear();
        commitBufferSegmentFieldInfosBuilder.put(commitBufferIndexKey, builder);
        commitBufferSegmentFieldInfos.clear();
        commitBufferSegmentFieldInfos.put(commitBufferIndexKey, builder.finish());
      }
    }

    return builder;
  }

/*
  private static class FieldCacheKey{
    //IndexReader.CacheKey readerCacheKey;
    IndexReader.CacheKey coreCacheKey;
    //String info;
    Directory directory;

    private FieldCacheKey(Directory directory, LeafReader leafReader) {
      this.coreCacheKey = leafReader.getCoreCacheHelper().getKey();
      //this.readerCacheKey = leafReader.getReaderCacheHelper().getKey();
      //this.info = leafReader.toString();
      this.directory = directory;
    }

    private Path path(){
      Directory unwrappedDir = FilterDirectory.unwrap(directory);
      return ((FSDirectory) unwrappedDir).getDirectory();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FieldCacheKey that = (FieldCacheKey) o;
      return Objects.equals(coreCacheKey, that.coreCacheKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(coreCacheKey);
    }

    @Override
    public String toString() {
      int index = info.indexOf("Uninverting(_");
      int txFilterCodecIndex = info.indexOf("TxFilterCodecReader(_");
      int txFilterLeafIndex = info.indexOf("TxFilterLeafReader(_");

      String type="";
      int start = 0;
      if(index > 0){
        start = index+12;
        type = "";
      }else if(txFilterCodecIndex > 0){
        start = txFilterCodecIndex + 20;
        type = "txc";
      }else if(txFilterLeafIndex > 0){
        start = txFilterLeafIndex + 19;
        type = "txl";
      }

      return new StringJoiner(", ", FieldCacheKey.class.getSimpleName() + "[", "]")
          .add("coreCacheKey=" + coreCacheKey)
          .add("readerCacheKey=" + readerCacheKey)
          .add("directory=" + directory)
          .add("info='" + type + ":" + info.substring(start, start + 5) + "'")
          .toString();
    }
  }
*/

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
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos>> privateIndexFieldInfoCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> mainSegmentFieldInfoBuilderCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> commitBufferFieldInfoBuilderCache = new ConcurrentHashMap<>();
  private static final Map<Directory, Map<Set<IndexReader.CacheKey>, FieldInfos.Builder>> privateIndexFieldInfoBuilderCache = new ConcurrentHashMap<>();

  public static FieldInfos getMergedFieldInfos(IndexReader reader){

    if(!(reader instanceof DirectoryReader)){
      return doGetMergedFieldInfos(reader).finish();
    }

    final DirectoryReader directoryReader = (DirectoryReader) reader;
    final Directory mainDirectory = directoryReader.directory();
    final Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> privateIndexSegmentFieldInfoBuilders = privateIndexFieldInfoBuilderCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> commitBufferSegmentFieldInfoBuilders = commitBufferFieldInfoBuilderCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> mainIndexSegmentFieldInfoBuilders = mainSegmentFieldInfoBuilderCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos> privateIndexSegmentFieldInfos = privateIndexFieldInfoCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos> commitBufferSegmentFieldInfos = commitBufferFieldInfoCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());
    final Map<Set<IndexReader.CacheKey>, FieldInfos> mainIndexSegmentFieldInfos = mainSegmentFieldInfoCache.computeIfAbsent(mainDirectory, d -> new ConcurrentHashMap<>());


    final Set<IndexReader.CacheKey> mainInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> commitBufferInputSegmentFields = new HashSet<>();
    final Set<IndexReader.CacheKey> privateIndexInputSegmentFields = new HashSet<>();
    final List<LeafReader> mainInputLeafReaders = new LinkedList<>();
    final List<LeafReader> commitBufferInputLeafReaders = new LinkedList<>();
    final List<LeafReader> privateIndexInputLeafReaders = new LinkedList<>();

    reader.leaves().forEach(ctx -> {
      // TODO: .filter(r -> r instanceof CodecReader) and .filter(r -> r instanceof SegmentReader) ?
      LeafReader leafReader = FilterLeafReader.unwrap(ctx.reader());
      SegmentReader segmentReader = (SegmentReader) FilterCodecReader.unwrap((CodecReader) leafReader);
      Directory unwrappedDir = FilterDirectory.unwrap(segmentReader.directory());
      Path segmentPath = ((FSDirectory) unwrappedDir).getDirectory();
      IndexReader.CacheKey cacheKey = segmentReader.getCoreCacheHelper().getKey();
      if(COMMIT_BUFFER_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        commitBufferInputLeafReaders.add(ctx.reader());
        commitBufferInputSegmentFields.add(cacheKey);
      }else if(PRIVATE_INDEX_DIR_PATTERN.matcher(segmentPath.toString()).find()){
        privateIndexInputLeafReaders.add(ctx.reader());
        privateIndexInputSegmentFields.add(cacheKey);
      }else{
        mainInputLeafReaders.add(ctx.reader());
        mainInputSegmentFields.add(cacheKey);
      }
      //System.out.println("Segment: " + segmentPath + "/" + segmentReader.getSegmentName());
    });

//    System.out.println("privateIndexFieldInfoCache: " + privateIndexSegmentFieldInfoBuilders.size());
//    System.out.println("commitBufferSegmentFieldInfoBuilders: " + commitBufferSegmentFieldInfoBuilders.size());
//    System.out.println("mainIndexSegmentFieldInfoBuilders: " + mainIndexSegmentFieldInfoBuilders.size());

    final Set<IndexReader.CacheKey> commitBufferIndexKey = new HashSet<>();
    commitBufferIndexKey.addAll(mainInputSegmentFields);
    commitBufferIndexKey.addAll(commitBufferInputSegmentFields);

    FieldInfos.Builder result;
    // This is a private index view with txId
    if(!privateIndexInputLeafReaders.isEmpty()) {
      final Set<IndexReader.CacheKey> privateIndexKey = new HashSet<>();
      privateIndexKey.addAll(mainInputSegmentFields);
      privateIndexKey.addAll(commitBufferInputSegmentFields);
      privateIndexKey.addAll(privateIndexInputSegmentFields);
      // We can have 1 cache entry per txId
      result = privateIndexSegmentFieldInfoBuilders.computeIfAbsent(privateIndexKey, key -> {
        FieldInfos.Builder builder = computeCommitBufferFieldInfosBuilder(reader,  mainInputSegmentFields, mainIndexSegmentFieldInfoBuilders, mainInputLeafReaders, commitBufferIndexKey, commitBufferSegmentFieldInfoBuilders, commitBufferInputLeafReaders, mainIndexSegmentFieldInfos, commitBufferSegmentFieldInfos);
        // Build private index multi fields from commitBufferMultiFields and store into cache
        for (LeafReader commitBufferReader : privateIndexInputLeafReaders) {
          synchronized (commitBufferSegmentFieldInfos) {
            builder = addSegmentFieldInfos(commitBufferReader.getContext(), builder);
          }
          // register on private index segment closed listener
          commitBufferReader.getCoreCacheHelper().addClosedListener(closedCacheKey -> {
            privateIndexSegmentFieldInfoBuilders.entrySet().removeIf(entry -> entry.getKey().contains(closedCacheKey));
            privateIndexSegmentFieldInfos.entrySet().removeIf(entry -> entry.getKey().contains(closedCacheKey));
          });
        }
        return builder;
      });

      return privateIndexSegmentFieldInfos.computeIfAbsent(privateIndexKey, key -> {
        synchronized (commitBufferSegmentFieldInfos){
          return result.finish();
        }
      });
      // This is a commit buffer only view without txid
    }else if (!commitBufferInputLeafReaders.isEmpty()){
      result = computeCommitBufferFieldInfosBuilder(reader, mainInputSegmentFields, mainIndexSegmentFieldInfoBuilders, mainInputLeafReaders, commitBufferIndexKey, commitBufferSegmentFieldInfoBuilders, commitBufferInputLeafReaders, mainIndexSegmentFieldInfos, commitBufferSegmentFieldInfos);
      FieldInfos fieldInfos = commitBufferSegmentFieldInfos.get(commitBufferIndexKey);
      if(fieldInfos == null){
        synchronized (commitBufferSegmentFieldInfos) {
          return result.finish();
        }
      }else{
        return fieldInfos;
      }
      // This is a main index only view
    }else{
      result = computeMainIndexFieldInfosBuilder(reader, mainInputSegmentFields, mainIndexSegmentFieldInfoBuilders, mainInputLeafReaders, mainIndexSegmentFieldInfos);
      FieldInfos fieldInfos = mainIndexSegmentFieldInfos.get(mainInputSegmentFields);
      return fieldInfos == null? result.finish() : fieldInfos;
    }
  }

  /** Returns a single {@link Bits} instance for this
   *  reader, merging live Documents on the
   *  fly.  This method will return null if the reader 
   *  has no deletions.
   *
   *  <p><b>NOTE</b>: this is a very slow way to access live docs.
   *  For example, each Bits access will require a binary search.
   *  It's better to get the sub-readers and iterate through them
   *  yourself. */
  public static Bits getLiveDocs(IndexReader reader) {
    if (reader.hasDeletions()) {
      final List<LeafReaderContext> leaves = reader.leaves();
      final int size = leaves.size();
      assert size > 0 : "A reader with deletions must have at least one leave";
      if (size == 1) {
        return leaves.get(0).reader().getLiveDocs();
      }
      final Bits[] liveDocs = new Bits[size];
      final int[] starts = new int[size + 1];
      for (int i = 0; i < size; i++) {
        // record all liveDocs, even if they are null
        final LeafReaderContext ctx = leaves.get(i);
        liveDocs[i] = ctx.reader().getLiveDocs();
        starts[i] = ctx.docBase;
      }
      starts[size] = reader.maxDoc();
      return new MultiBits(liveDocs, starts, true);
    } else {
      return null;
    }
  }

  /** This method may return null if the field does not exist or if it has no terms. */
  public static Terms getTerms(IndexReader r, String field) throws IOException {
    final List<LeafReaderContext> leaves = r.leaves();
    if (leaves.size() == 1) {
      return leaves.get(0).reader().terms(field);
    }

    final List<Terms> termsPerLeaf = new ArrayList<>(leaves.size());
    final List<ReaderSlice> slicePerLeaf = new ArrayList<>(leaves.size());

    for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
      LeafReaderContext ctx = leaves.get(leafIdx);
      Terms subTerms = ctx.reader().terms(field);
      if (subTerms != null) {
        termsPerLeaf.add(subTerms);
        slicePerLeaf.add(new ReaderSlice(ctx.docBase, r.maxDoc(), leafIdx - 1));
      }
    }

    if (termsPerLeaf.size() == 0) {
      return null;
    } else {
      return new MultiTerms(termsPerLeaf.toArray(Terms.EMPTY_ARRAY),
          slicePerLeaf.toArray(ReaderSlice.EMPTY_ARRAY));
    }
  }
  
  /** Returns {@link PostingsEnum} for the specified field and
   *  term.  This will return null if the field or term does
   *  not exist. */
  public static PostingsEnum getTermDocsEnum(IndexReader r, String field, BytesRef term) throws IOException {
    return getTermDocsEnum(r, field, term, PostingsEnum.FREQS);
  }
  
  /** Returns {@link PostingsEnum} for the specified field and
   *  term, with control over whether freqs are required.
   *  Some codecs may be able to optimize their
   *  implementation when freqs are not required.  This will
   *  return null if the field or term does not exist.  See {@link
   *  TermsEnum#postings(PostingsEnum,int)}.*/
  public static PostingsEnum getTermDocsEnum(IndexReader r, String field, BytesRef term, int flags) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term)) {
        return termsEnum.postings(null, flags);
      }
    }
    return null;
  }

  /** Returns {@link PostingsEnum} for the specified
   *  field and term.  This will return null if the field or
   *  term does not exist or positions were not indexed. 
   *  @see #getTermPositionsEnum(IndexReader, String, BytesRef, int) */
  public static PostingsEnum getTermPositionsEnum(IndexReader r, String field, BytesRef term) throws IOException {
    return getTermPositionsEnum(r, field, term, PostingsEnum.ALL);
  }

  /** Returns {@link PostingsEnum} for the specified
   *  field and term, with control over whether offsets and payloads are
   *  required.  Some codecs may be able to optimize
   *  their implementation when offsets and/or payloads are not
   *  required. This will return null if the field or term does not
   *  exist. See {@link TermsEnum#postings(PostingsEnum,int)}. */
  public static PostingsEnum getTermPositionsEnum(IndexReader r, String field, BytesRef term, int flags) throws IOException {
    assert field != null;
    assert term != null;
    final Terms terms = getTerms(r, field);
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      if (termsEnum.seekExact(term)) {
        return termsEnum.postings(null, flags);
      }
    }
    return null;
  }

  /**
   * Expert: construct a new MultiFields instance directly.
   * @lucene.internal
   */
  // TODO: why is this public?
  public MultiFields(Fields[] subs, ReaderSlice[] subSlices) {
    this.subs = subs;
    this.subSlices = subSlices;
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  @Override
  public Iterator<String> iterator() {
    Iterator<String> subIterators[] = new Iterator[subs.length];
    for(int i=0;i<subs.length;i++) {
      subIterators[i] = subs[i].iterator();
    }
    return new MergedIterator<>(subIterators);
  }

  @Override
  public Terms terms(String field) throws IOException {
    Terms result = terms.get(field);
    if (result != null)
      return result;


    // Lazy init: first time this field is requested, we
    // create & add to terms:
    final List<Terms> subs2 = new ArrayList<>();
    final List<ReaderSlice> slices2 = new ArrayList<>();

    // Gather all sub-readers that share this field
    for(int i=0;i<subs.length;i++) {
      final Terms terms = subs[i].terms(field);
      if (terms != null) {
        subs2.add(terms);
        slices2.add(subSlices[i]);
      }
    }
    if (subs2.size() == 0) {
      result = null;
      // don't cache this case with an unbounded cache, since the number of fields that don't exist
      // is unbounded.
    } else {
      result = new MultiTerms(subs2.toArray(Terms.EMPTY_ARRAY),
          slices2.toArray(ReaderSlice.EMPTY_ARRAY));
      terms.put(field, result);
    }

    return result;
  }

  @Override
  public int size() {
    return -1;
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

  /** Call this to get the (merged) FieldInfos representing the
   *  set of indexed fields <b>only</b> for a composite reader. 
   *  <p>
   *  NOTE: the returned field numbers will likely not
   *  correspond to the actual field numbers in the underlying
   *  readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   *  will be unavailable.
   */
  public static Collection<String> getIndexedFields(IndexReader reader) {
    final Collection<String> fields = new HashSet<>();
    for(final FieldInfo fieldInfo : getMergedFieldInfos(reader)) {
      if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
        fields.add(fieldInfo.name);
      }
    }
    return fields;
  }

  private static class LeafReaderFields extends Fields {

    private final LeafReader leafReader;
    private final List<String> indexedFields;

    LeafReaderFields(LeafReader leafReader) {
      this.leafReader = leafReader;
      this.indexedFields = new ArrayList<>();
      for (FieldInfo fieldInfo : leafReader.getFieldInfos()) {
        if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
          indexedFields.add(fieldInfo.name);
        }
      }
      Collections.sort(indexedFields);
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableList(indexedFields).iterator();
    }

    @Override
    public int size() {
      return indexedFields.size();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return leafReader.terms(field);
    }
  }
}

