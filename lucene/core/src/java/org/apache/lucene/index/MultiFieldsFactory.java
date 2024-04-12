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

public class MultiFieldsFactory {
  public static Fields getFields(IndexReader reader) throws IOException{

    IndexReader.CacheHelper baseCacheHelper = reader.getReaderCacheHelper();
    if(isMultiDirectoryReader(reader, baseCacheHelper)){
      return MultiFieldsCacheWrapper.getFields(reader);
    }
    return MultiFields.getFields(reader);
  }
  public static FieldInfos getMergedFieldInfos(IndexReader reader){
    IndexReader.CacheHelper baseCacheHelper = reader.getReaderCacheHelper();
    if(isMultiDirectoryReader(reader, baseCacheHelper)){
      return MultiFieldsCacheWrapper.getMergedFieldInfos(reader);
    }
    return MultiFields.getMergedFieldInfos(reader);
  }

  private static boolean isMultiDirectoryReader(IndexReader reader, IndexReader.CacheHelper baseCacheHelper) {
    return (reader instanceof DirectoryReader) && baseCacheHelper != null && baseCacheHelper.toString().startsWith("MultiDirectoryReader");
  }
}
