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

package org.apache.kylin.common.persistence;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

abstract public class ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStore.class);

    public static final String CUBE_RESOURCE_ROOT = "/cube";
    public static final String II_RESOURCE_ROOT = "/invertedindex";
    public static final String CUBE_DESC_RESOURCE_ROOT = "/cube_desc";
    public static final String II_DESC_RESOURCE_ROOT = "/invertedindex_desc";
    public static final String DATA_MODEL_DESC_RESOURCE_ROOT = "/model_desc";
    public static final String DICT_RESOURCE_ROOT = "/dict";
    public static final String JOB_PATH_ROOT = "/job";
    public static final String JOB_OUTPUT_PATH_ROOT = "/job_output";
    public static final String PROJECT_RESOURCE_ROOT = "/project";
    public static final String SNAPSHOT_RESOURCE_ROOT = "/table_snapshot";
    public static final String TABLE_EXD_RESOURCE_ROOT = "/table_exd";
    public static final String TABLE_RESOURCE_ROOT = "/table";
    public static final String HYBRID_RESOURCE_ROOT = "/hybrid";
    public static final String EXECUTE_PATH_ROOT = "/execute";
    public static final String EXECUTE_OUTPUT_ROOT = "/execute_output";


    private static ConcurrentHashMap<KylinConfig, ResourceStore> CACHE = new ConcurrentHashMap<KylinConfig, ResourceStore>();

    public static final ArrayList<Class<? extends ResourceStore>> knownImpl = new ArrayList<Class<? extends ResourceStore>>();

    static {
        knownImpl.add(FileResourceStore.class);
        knownImpl.add(HBaseResourceStore.class);
    }

    public static ResourceStore getStore(KylinConfig kylinConfig) {
        ResourceStore r = CACHE.get(kylinConfig);
        List<Throwable> es = new ArrayList<Throwable>();
        if (r == null) {
            logger.info("Using metadata url " + kylinConfig.getMetadataUrl() + " for resource store");
            for (Class<? extends ResourceStore> cls : knownImpl) {

                try {
                    r = cls.getConstructor(KylinConfig.class).newInstance(kylinConfig);
                } catch (Exception e) {
                    es.add(e);
                } catch (NoClassDefFoundError er) {
                    // may throw NoClassDefFoundError
                    es.add(er);
                }
                if (r != null) {
                    break;
                }
            }
            if (r == null) {
                for (Throwable exceptionOrError : es) {
                    logger.error("Create new store instance failed ", exceptionOrError);
                }
                throw new IllegalArgumentException("Failed to find metadata store by url: " + kylinConfig.getMetadataUrl());
            }

            CACHE.put(kylinConfig, r);
        }
        return r;
    }

    // ============================================================================

    KylinConfig kylinConfig;

    ResourceStore(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    /**
     * return a list of child resources & folders under given path, return null
     * if given path is not a folder
     */
    final public ArrayList<String> listResources(String resPath) throws IOException {
        resPath = norm(resPath);
        return listResourcesImpl(resPath);
    }

    abstract protected ArrayList<String> listResourcesImpl(String resPath) throws IOException;

    /**
     * return true if a resource exists, return false in case of folder or
     * non-exist
     */
    final public boolean exists(String resPath) throws IOException {
        return existsImpl(norm(resPath));
    }

    abstract protected boolean existsImpl(String resPath) throws IOException;

    /**
     * read a resource, return null in case of not found
     */
    final public <T extends RootPersistentEntity> T getResource(String resPath, Class<T> clz, Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        RawResource res = getResourceImpl(resPath);
        if (res == null)
            return null;
        
        DataInputStream din = new DataInputStream(res.inputStream);
        try {
            T r = serializer.deserialize(din);
            r.setLastModified(res.timestamp);
            return r;
        } finally {
            IOUtils.closeQuietly(din);
            IOUtils.closeQuietly(res.inputStream);
        }
    }

    final public RawResource getResource(String resPath) throws IOException {
        return getResourceImpl(norm(resPath));
    }

    final public long getResourceTimestamp(String resPath) throws IOException {
        return getResourceTimestampImpl(norm(resPath));
    }
    
    final public <T extends RootPersistentEntity> List<T> getAllResources(String rangeStart, String rangeEnd, Class<T> clazz, Serializer<T> serializer) throws IOException {
        final List<RawResource> allResources = getAllResources(rangeStart, rangeEnd);
        if (allResources.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = Lists.newArrayList();
        try {
            for (RawResource rawResource : allResources) {
                final T element = serializer.deserialize(new DataInputStream(rawResource.inputStream));
                element.setLastModified(rawResource.timestamp);
                result.add(element);
            }
            return result;
        } finally {
            for (RawResource rawResource : allResources) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
        }
    }

    abstract protected List<RawResource> getAllResources(String rangeStart, String rangeEnd) throws IOException;

    /** returns null if not exists */
    abstract protected RawResource getResourceImpl(String resPath) throws IOException;
    
    /** returns 0 if not exists */
    abstract protected long getResourceTimestampImpl(String resPath) throws IOException;
    
    /**
     * overwrite a resource without write conflict check
     */
    final public void putResource(String resPath, InputStream content, long ts) throws IOException {
        resPath = norm(resPath);
        logger.debug("Saving resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
        putResourceImpl(resPath, content, ts);
    }

    abstract protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException;

    /**
     * check & set, overwrite a resource
     */
    final public <T extends RootPersistentEntity> long putResource(String resPath, T obj, Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        logger.debug("Saving resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");

        long oldTS = obj.getLastModified();
        long newTS = System.currentTimeMillis();
        obj.setLastModified(newTS);

        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            serializer.serialize(obj, dout);
            dout.close();
            buf.close();

            newTS = checkAndPutResourceImpl(resPath, buf.toByteArray(), oldTS, newTS);
            obj.setLastModified(newTS); // update again the confirmed TS
            return newTS;
        } catch (IOException e) {
            obj.setLastModified(oldTS); // roll back TS when write fail
            throw e;
        } catch (RuntimeException e) {
            obj.setLastModified(oldTS); // roll back TS when write fail
            throw e;
        }
    }

    /**
     * checks old timestamp when overwriting existing
     */
    abstract protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException;

    /**
     * delete a resource, does nothing on a folder
     */
    final public void deleteResource(String resPath) throws IOException {
        logger.debug("Deleting resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
        deleteResourceImpl(norm(resPath));
    }

    abstract protected void deleteResourceImpl(String resPath) throws IOException;

    /**
     * get a readable string of a resource path
     */
    final public String getReadableResourcePath(String resPath) {
        return getReadableResourcePathImpl(norm(resPath));
    }

    abstract protected String getReadableResourcePathImpl(String resPath);

    private String norm(String resPath) {
        resPath = resPath.trim();
        while (resPath.startsWith("//"))
            resPath = resPath.substring(1);
        while (resPath.endsWith("/"))
            resPath = resPath.substring(0, resPath.length() - 1);
        if (resPath.startsWith("/") == false)
            resPath = "/" + resPath;
        return resPath;
    }

    // ============================================================================

    public static interface Visitor {
        void visit(String path) throws IOException;
    }

    public void scanRecursively(String path, Visitor visitor) throws IOException {
        ArrayList<String> children = listResources(path);
        if (children != null) {
            for (String child : children)
                scanRecursively(child, visitor);
            return;
        }

        if (exists(path))
            visitor.visit(path);
    }

    public List<String> collectResourceRecursively(String root, final String suffix) throws IOException {
        final ArrayList<String> collector = Lists.newArrayList();
        scanRecursively(root, new Visitor() {
            @Override
            public void visit(String path) {
                if (path.endsWith(suffix))
                    collector.add(path);
            }
        });
        return collector;
    }

}
