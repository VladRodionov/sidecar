/*
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
package com.carrot.sidecar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import com.carrot.cache.Builder;
import com.carrot.cache.Cache;
import com.carrot.cache.Scavenger;
import com.carrot.cache.Scavenger.Stats;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.index.CompactBaseIndexFormat;
import com.carrot.cache.io.BaseDataWriter;
import com.carrot.cache.io.BaseFileDataReader;
import com.carrot.cache.io.BaseMemoryDataReader;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.sidecar.util.CacheType;
import com.carrot.sidecar.util.SidecarConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  public static File createTempFile() throws IOException {
    File f = File.createTempFile("carrot-temp", null);
    return f;
  }

  public static File createTempFile(String dir) throws IOException {
    String path = dir + File.separator + System.nanoTime();
    return new File(path);
  }
  
  public static void fillRandom(File f, long size) throws IOException {
    Random r = new Random();
    FileOutputStream fos = new FileOutputStream(f);
    long written = 0;
    byte[] buffer = new byte[1024 * 1024];
    while (written < size) {
      r.nextBytes(buffer);
      int toWrite = (int) Math.min(buffer.length, size - written);
      fos.write(buffer, 0, toWrite);
      written += toWrite;
    }
    fos.close();
  }

  public static void touchFileAt(File f, long offset) throws IOException {
    Random r = new Random();
    long value = r.nextLong();
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek(offset);
    raf.writeLong(value);
    raf.close();
  }
  /**
   * Deletes a file or a directory, recursively if it is a directory.
   *
   * If the path does not exist, nothing happens.
   *
   * @param path pathname to be deleted
   */
  public static void deletePathRecursively(String path) throws IOException {
    Path root = Paths.get(path);

    if (!Files.exists(root)) {
      return;
    }
    
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
        if (e == null) {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        } else {
          throw e;
        }
      }
    });
  }
  
  public static Configuration getHdfsConfiguration(SidecarConfig sidecarConfig, CarrotConfig carrotCacheConfig)
  {
      Configuration configuration = new Configuration();
      for(Entry<Object, Object> e: sidecarConfig.entrySet()) {
        configuration.set((String) e.getKey(), (String) e.getValue());
      }
      Properties p = carrotCacheConfig.getProperties();
      for(Entry<Object, Object> e: p.entrySet()) {
        configuration.set((String) e.getKey(), (String) e.getValue());
      }
      return configuration;
  }
  
  public static Cache createDataCacheFromHdfsConfiguration(Configuration configuration) throws IOException {
    CarrotConfig config = CarrotConfig.getInstance(); 
    Iterator<Map.Entry<String, String>> it = configuration.iterator();
    while(it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String name = entry.getKey();
      if (CarrotConfig.isCarrotPropertyName(name)) {
        config.setProperty(name, entry.getValue());
      }
    }
    setCacheTypes(config);
    config.setStartIndexNumberOfSlotsPower(SidecarConfig.DATA_CACHE_FILE_NAME, 4);
    config.setStartIndexNumberOfSlotsPower(SidecarConfig.DATA_CACHE_OFFHEAP_NAME, 4);
    config.setStartIndexNumberOfSlotsPower(SidecarConfig.META_CACHE_NAME, 4);
    SidecarConfig sconfig = SidecarConfig.fromHadoopConfiguration(configuration);
    CacheType cacheType = sconfig.getDataCacheType();
    return createCache(config, cacheType);
  }
  
  private static Cache createCache(CarrotConfig config, CacheType cacheType) throws IOException {
    switch (cacheType) {
      case FILE: 
      case OFFHEAP:
        return new Cache(cacheType.getCacheName(), config);
      case HYBRID:
        Cache parent = new Cache(CacheType.OFFHEAP.getCacheName(), config);
        Cache victim = new Cache(CacheType.FILE.getCacheName(), config);
        parent.setVictimCache(victim);
        return parent;
    }
    return null;
  }

  private static void setCacheTypes(CarrotConfig config) {
    String[] names = config.getCacheNames();
    String[] types = config.getCacheTypes();
    
    String[] newNames = new String[names.length + 3];
    System.arraycopy(names, 0, newNames, 0, names.length);
    
    newNames[newNames.length - 3] = SidecarConfig.DATA_CACHE_FILE_NAME;
    newNames[newNames.length - 2] = SidecarConfig.DATA_CACHE_OFFHEAP_NAME;
    newNames[newNames.length - 1] = SidecarConfig.META_CACHE_NAME;
    
    
    String[] newTypes = new String[types.length + 3];
    System.arraycopy(types, 0, newTypes, 0, types.length);
    newTypes[newTypes.length - 3] = CacheType.FILE.getType();
    newTypes[newTypes.length - 2] = CacheType.OFFHEAP.getType();
    newTypes[newTypes.length - 1] = CacheType.OFFHEAP.getType();;
    
    
    String cacheNames = com.carrot.sidecar.util.Utils.join(newNames, ",");
    String cacheTypes = com.carrot.sidecar.util.Utils.join(newTypes, ",");
    config.setCacheNames(cacheNames);
    config.setCacheTypes(cacheTypes);
  }
  
  public static Cache createMetaCacheFromHdfsConfiguration(Configuration configuration)
      throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
    Iterator<Map.Entry<String, String>> it = configuration.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String name = entry.getKey();
      if (CarrotConfig.isCarrotPropertyName(name)) {
        config.setProperty(name, entry.getValue());
      }
    }
    
    setCacheTypes(config);
    
    Cache metaCache;
    long maxSize = config.getCacheMaximumSize(SidecarConfig.META_CACHE_NAME);
    int dataSegmentSize = (int) config.getCacheSegmentSize(SidecarConfig.META_CACHE_NAME);
    Builder builder = new Builder(SidecarConfig.META_CACHE_NAME);
    builder = builder

        .withCacheMaximumSize(maxSize).withCacheDataSegmentSize(dataSegmentSize)
        .withRecyclingSelector(LRCRecyclingSelector.class.getName())
        .withDataWriter(BaseDataWriter.class.getName())
        .withMemoryDataReader(BaseMemoryDataReader.class.getName())
        .withFileDataReader(BaseFileDataReader.class.getName())
        .withMainQueueIndexFormat(CompactBaseIndexFormat.class.getName());
    metaCache = builder.buildMemoryCache();
    return metaCache;
  }

  public static void printStats(Cache cache) {
    LOG.info("Cache[{}]: storage size={} data size={} items={} hit rate={}, puts={}, bytes written={}",
             cache.getName(), cache.getStorageAllocated(), cache.getStorageUsed(), cache.size(),
             cache.getHitRate(), cache.getTotalWrites(), cache.getTotalWritesSize());
    Stats stats = Scavenger.getStatisticsForCache(cache.getName());
    LOG.info("Scavenger [{}]: runs={} scanned={} freed={} written back={} empty segments={}", 
      cache.getName(), stats.getTotalRuns(), stats.getTotalBytesScanned(), 
      stats.getTotalBytesFreed(), stats.getTotalBytesScanned() - stats.getTotalBytesFreed(),
      stats.getTotalEmptySegments());
    
    cache = cache.getVictimCache();
    if (cache != null) {
      LOG.info("Cache[{}]: storage size={} data size={} items={} hit rate={}, puts={}, bytes written={}\n",
        cache.getName(), cache.getStorageAllocated(), cache.getStorageUsed(), cache.size(),
        cache.getHitRate(), cache.getTotalWrites(), cache.getTotalWritesSize());
      stats = Scavenger.getStatisticsForCache(cache.getName());
      LOG.info("Scavenger [{}]: runs={} scanned={} freed={} written back={} empty segments={}\n", 
        cache.getName(), stats.getTotalRuns(), stats.getTotalBytesScanned(), 
        stats.getTotalBytesFreed(), stats.getTotalBytesScanned() - stats.getTotalBytesFreed(),
        stats.getTotalEmptySegments());
    }
  }
}
