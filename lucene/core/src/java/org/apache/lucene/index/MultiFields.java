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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.Optional;

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
  private static Fields doGetFields(IndexReader reader) throws IOException {
    final List<LeafReaderContext> leaves = reader.leaves();
    switch (leaves.size()) {
      case 1:
        // already an atomic reader / reader with one leave
        return new LeafReaderFields(leaves.get(0).reader());
      default:
        final List<Fields> fields = new ArrayList<>(leaves.size());
        final List<ReaderSlice> slices = new ArrayList<>(leaves.size());
        for (final LeafReaderContext ctx : leaves) {
          final LeafReader r = ctx.reader();
          final Fields f = new LeafReaderFields(r);
          fields.add(f);
          slices.add(new ReaderSlice(ctx.docBase, r.maxDoc(), fields.size()-1));
        }
        if (fields.size() == 1) {
          return fields.get(0);
        } else {
          return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                         slices.toArray(ReaderSlice.EMPTY_ARRAY));
        }
    }
  }

  private static Map<Set<IndexReader.CacheKey>, Fields> staticSegmentFields = new ConcurrentHashMap<>();
  private static Fields addSegmentFields(LeafReaderContext ctx, List<Fields> fields, List<ReaderSlice> slices){
    final LeafReader r = ctx.reader();
    final Fields f = new MultiFields.LeafReaderFields(r);
    fields.add(f);
    slices.add(new ReaderSlice(ctx.docBase, r.maxDoc(), fields.size()-1));
    if (fields.size() == 1) {
      return fields.get(0);
    } else {
      return new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
          slices.toArray(ReaderSlice.EMPTY_ARRAY));
    }
  }
  public static Fields getFields(IndexReader reader) throws IOException {
    Fields newFields = null;
    Fields oldFields = null;
    Optional<Map.Entry<Set<IndexReader.CacheKey>, Fields>> existingSegmentFieldsOptional = staticSegmentFields.entrySet().stream().findFirst();
    final Set<IndexReader.CacheKey> inputSegmentFieldsCacheKey = reader.leaves().stream().map(ctx -> ctx.reader().getCoreCacheHelper().getKey()).collect(Collectors.toSet());

    if (existingSegmentFieldsOptional.isPresent()) {
      final Map.Entry<Set<IndexReader.CacheKey>, Fields> existingSegmentFields = existingSegmentFieldsOptional.get();
      Fields fields = null;

      // find input leaf readers that are not in the cache
      List<LeafReaderContext> newLeafReaderFieldsContexts = reader.leaves().stream().filter(ctx -> !existingSegmentFields.getKey().contains(ctx.reader().getCoreCacheHelper().getKey())).collect(Collectors.toList());

      // if there's no merge that caused some segment to be gone since the last time we cache
      boolean noSegmentMerged = inputSegmentFieldsCacheKey.containsAll(existingSegmentFields.getKey());

      // if there's no new segments and our existing cached segments are matching the input (segments not merged)
      if (inputSegmentFieldsCacheKey.equals(existingSegmentFields.getKey())) {
        newFields = existingSegmentFields.getValue();
      } else if(!noSegmentMerged){
        // some cached segments are merged, we need to rebuild
        synchronized (staticSegmentFields) {
          oldFields = staticSegmentFields.remove(existingSegmentFields.getKey());
        }
      } else if(!newLeafReaderFieldsContexts.isEmpty()){
        // new segments are introduced not already cached
        synchronized (staticSegmentFields) {
          existingSegmentFieldsOptional = staticSegmentFields.entrySet().stream().findFirst();
          if(existingSegmentFieldsOptional.isPresent()) {
            final Map.Entry<Set<IndexReader.CacheKey>, Fields> synchronizedExistingSegmentFields = existingSegmentFieldsOptional.get();
            newLeafReaderFieldsContexts = reader.leaves().stream().filter(ctx -> !synchronizedExistingSegmentFields.getKey().contains(ctx.reader().getCoreCacheHelper().getKey())).collect(Collectors.toList());
            if (!newLeafReaderFieldsContexts.isEmpty()) {
              // if there are changes to segments, we need to find the diff and call addSegmentFields()
              fields = synchronizedExistingSegmentFields.getValue();
              if (fields instanceof MultiFields) {
                MultiFields multiFields = (MultiFields) fields;
                staticSegmentFields.remove(synchronizedExistingSegmentFields.getKey());
                for (LeafReaderContext leafReaderContext : newLeafReaderFieldsContexts) {
                  newFields = addSegmentFields(leafReaderContext, new ArrayList<>(Arrays.asList(multiFields.subs)), new ArrayList<>(Arrays.asList(multiFields.subSlices)));
                }
                staticSegmentFields.put(inputSegmentFieldsCacheKey, newFields);
              }
            }
          }
        }
      }
    }

    if(newFields != null){
      return newFields;
    }

    newFields = staticSegmentFields.computeIfAbsent(inputSegmentFieldsCacheKey, key -> {
      try {
        return doGetFields(reader);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
/*
    // we can only close whatever leaf reader that's no longer passed as leaf of the input CompositeReader
    if(oldFields instanceof MultiFields){
      Set<IndexReader.CacheKey> newCacheKeys = new HashSet<>();
      if(newFields instanceof MultiFields){
        for(Fields field : ((MultiFields)newFields).subs){
          if(field instanceof LeafReaderFields){
            LeafReaderFields leafReaderFields = (LeafReaderFields) field;
            newCacheKeys.add(leafReaderFields.leafReader.getCoreCacheHelper().getKey());
          }
        }
      }else{
        newCacheKeys.add(((LeafReaderFields) newFields).leafReader.getCoreCacheHelper().getKey());
      }
      MultiFields oldMultiFields = (MultiFields) oldFields;
      for(Fields field : oldMultiFields.subs){
        if(field instanceof LeafReaderFields){
          LeafReaderFields leafReaderFields = (LeafReaderFields) field;
          if(!newCacheKeys.contains(leafReaderFields.leafReader.getCoreCacheHelper().getKey())) {
            // decRef() old readers which were incRef() by addSegmentFields() and is no longer referenced now
            leafReaderFields.close();
          }
        }
      }
    }
 */
    return newFields;
  }


  private static Map<Set<IndexReader.CacheKey>, FieldInfos.Builder> staticSegmentFieldInfosBuilder = new ConcurrentHashMap<>();
  private static Map<Set<IndexReader.CacheKey>, FieldInfos> staticSegmentFieldInfos = new ConcurrentHashMap<>();

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

  public static FieldInfos getMergedFieldInfos(IndexReader reader){

    Optional<Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder>> existingSegmentFieldInfosOptional = staticSegmentFieldInfosBuilder.entrySet().stream().findFirst();
    final Set<IndexReader.CacheKey> inputSegmentFieldsCacheKey = reader.leaves().stream().map(ctx -> ctx.reader().getCoreCacheHelper().getKey()).collect(Collectors.toSet());

    if(existingSegmentFieldInfosOptional.isPresent()){
      final Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> existingSegmentFieldInfos = existingSegmentFieldInfosOptional.get();
      FieldInfos.Builder builder = existingSegmentFieldInfos.getValue();

      // find input leaf readers that are not in the cache
      List<LeafReaderContext> newLeafReaderFieldInfosContexts = reader.leaves().stream().filter(ctx -> !existingSegmentFieldInfos.getKey().contains(ctx.reader().getCoreCacheHelper().getKey())).collect(Collectors.toList());

      // if there's no merge that caused some segment to be gone since the last time we cache
      boolean noSegmentMerged = inputSegmentFieldsCacheKey.containsAll(existingSegmentFieldInfos.getKey());

      // if there's no new segments and our existing cached segments are matching the input (segments not merged)
      if (inputSegmentFieldsCacheKey.equals(existingSegmentFieldInfos.getKey())) {
        final FieldInfos.Builder finalBuilder = builder;
        return staticSegmentFieldInfos.computeIfAbsent(inputSegmentFieldsCacheKey, key -> finalBuilder.finish());
      }else if(!noSegmentMerged){
        // some cached segments are merged, we need to rebuild
        synchronized (staticSegmentFieldInfosBuilder) {
          staticSegmentFieldInfos.remove(existingSegmentFieldInfos.getKey());
          staticSegmentFieldInfosBuilder.remove(existingSegmentFieldInfos.getKey());
        }
      } else if(!newLeafReaderFieldInfosContexts.isEmpty()){
        // new segments are introduced not already cached
        synchronized (staticSegmentFieldInfosBuilder) {
          existingSegmentFieldInfosOptional = staticSegmentFieldInfosBuilder.entrySet().stream().findFirst();
          if(existingSegmentFieldInfosOptional.isPresent()) {
            final Map.Entry<Set<IndexReader.CacheKey>, FieldInfos.Builder> synchronizedExistingSegmentFieldInfos = existingSegmentFieldInfosOptional.get();
            newLeafReaderFieldInfosContexts = reader.leaves().stream().filter(ctx -> !synchronizedExistingSegmentFieldInfos.getKey().contains(ctx.reader().getCoreCacheHelper().getKey())).collect(Collectors.toList());
            if (!newLeafReaderFieldInfosContexts.isEmpty()) {
              staticSegmentFieldInfos.remove(synchronizedExistingSegmentFieldInfos.getKey());
              staticSegmentFieldInfosBuilder.remove(synchronizedExistingSegmentFieldInfos.getKey());

              // if there are changes to segments, we need to find the diff and call addSegmentFieldInfos()
              for (LeafReaderContext leafReaderContext : newLeafReaderFieldInfosContexts) {
                builder = addSegmentFieldInfos(leafReaderContext, builder);
              }
              staticSegmentFieldInfosBuilder.put(inputSegmentFieldsCacheKey, builder);
              final FieldInfos.Builder finalBuilder = builder;
              return staticSegmentFieldInfos.computeIfAbsent(inputSegmentFieldsCacheKey, key -> finalBuilder.finish());
            }
          }
        }
      }
    }
    return staticSegmentFieldInfos.computeIfAbsent(inputSegmentFieldsCacheKey, key -> staticSegmentFieldInfosBuilder.computeIfAbsent(inputSegmentFieldsCacheKey, key2 -> doGetMergedFieldInfos(reader)).finish());
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
  private static FieldInfos.Builder doGetMergedFieldInfos(IndexReader reader) {
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
        builder.add(ctx.reader().getFieldInfos());
      }
      return builder;
    }
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
      //this.leafReader.incRef();
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
//    public void close() throws IOException {
//      leafReader.decRef();
//    }
  }
}

