/**
 * Copyright 2005 The Apache Software Foundation
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
 */
package org.apache.hadoop.dfs;

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 * 
 * @author Mike Cafarella
 *************************************************/
class FSDirectory implements FSConstants {
    static String FS_IMAGE = "fsimage";
    static String NEW_FS_IMAGE = "fsimage.new";
    static String OLD_FS_IMAGE = "fsimage.old";

    private static final byte OP_ADD = 0;
    private static final byte OP_RENAME = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_MKDIR = 3;

    /******************************************************
     * We keep an in-memory representation of the file/block
     * hierarchy.
     ******************************************************/
    class INode {
        public String name;
        public INode parent;
        // map是 <string，inode> 对
        public TreeMap children = new TreeMap();
        public Block blocks[];

        /**
         */
        INode(String name, INode parent, Block blocks[]) {
            this.name = name;
            this.parent = parent;
            this.blocks = blocks;
        }

        /**
         * Check whether it's a directory
         * @return
         */
        // blocks数组为空，则为dir
        synchronized public boolean isDir() {
          return (blocks == null);
        }

        /**
         * This is the external interface
         */
        INode getNode(String target) {
            if (! target.startsWith("/") || target.length() == 0) {
                return null;
            } else if (parent == null && "/".equals(target)) {
                return this;    // 获取根节点
            } else {
                Vector components = new Vector();
                int start = 0;
                int slashid = 0;
                while (start < target.length() && (slashid = target.indexOf('/', start)) >= 0) {
                    components.add(target.substring(start, slashid));     // 根据“/”将路径划分
                    start = slashid + 1;
                }
                if (start < target.length()) {
                    components.add(target.substring(start));
                }
                // 根据list里面的路径获得inode，从第一个名字开始，比如list里面存着 “user”，“jack”，“test”
                // 就先找 "user" 的inode，然后再底下再找“jack”的inode
                return getNode(components, 0);
            }
        }

        /**
         */
        INode getNode(Vector components, int index) {
            // 第0层，名字不匹配，直接返回null
            if (! name.equals((String) components.elementAt(index))) {
                return null;
            }
            // 最后一个了，必须是自己了
            if (index == components.size()-1) {
                return this;
            }
            // 在儿子中找下一个节点
            // Check with children
            INode child = (INode) children.get(components.elementAt(index+1));
            if (child == null) {
                // 找不到就返回null
                return null;
            } else {
                return child.getNode(components, index+1);
            }
        }

        /**
         */
        // 在target位置添加一个新的INode，并且添加blocks给它
        INode addNode(String target, Block blks[]) {
            // 已经存在了，返回null，不搞了
            if (getNode(target) != null) {
                return null;
            } else {
                String parentName = DFSFile.getDFSParent(target);
                // 父亲文件夹
                if (parentName == null) {
                    return null;
                }

                INode parentNode = getNode(parentName);
                if (parentNode == null) {
                    return null;
                } else {
                    String targetName = new File(target).getName();
                    // 创建一个INode
                    INode newItem = new INode(targetName, parentNode, blks);
                    // 直接在parent底下添加target文件
                    parentNode.children.put(targetName, newItem);
                    return newItem;   // 返回新建的INode
                }
            }
        }

        /**
         */
        boolean removeNode() {
            if (parent == null) {
                return false;
            } else {
                // 从parent的children中把自己删掉，自己以及自己底下的chiuldren会被GC掉
                parent.children.remove(name);
                return true;
            }
        }

        /**
         * Collect all the blocks at this INode and all its children.
         * This operation is performed after a node is removed from the tree,
         * and we want to GC all the blocks at this node and below.
         */
        void collectSubtreeBlocks(Vector v) {
            if (blocks != null) {
                // 采集当前的blocks
                for (int i = 0; i < blocks.length; i++) {
                    v.add(blocks[i]);
                }
            }
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                // 采集孩子们的blocks
                child.collectSubtreeBlocks(v);
            }
        }

        /**
         */
        // 统计有多少个INode节点
        int numItemsInTree() {
            int total = 0;
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.numItemsInTree();
            }
            return total + 1;
        }

        /**
         */
        String computeName() {
            // 从当前name，往上走到根节点得到全路径名
            if (parent != null) {
                return parent.computeName() + "/" + name;
            } else {
                return name;
            }
        }

        /**
         */
        // 计算当前文件的大小，把blocks大小相加即可
        long computeFileLength() {
            long total = 0;
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    total += blocks[i].getNumBytes();
                }
            }
            return total;
        }

        /**
         */
        // 计算当前及子树中所有blocks的大小，一般当前为文件夹
        long computeContentsLength() {
            long total = computeFileLength();
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.computeContentsLength();
            }
            return total;
        }

        /**
         */
        void listContents(Vector v) {
            if (parent != null && blocks != null) {
                v.add(this);
            }

            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                v.add(child);
            }
        }

        /**
         */
        // 递归的将INode树写入output流中，先写文件夹/文件名，再写block数目，再写block
        // 写入image的信息满足INode的DFS序，所以，重构INode树的时候，是按照DFS序重构的，每次
        // 找到parent级，然后添加child即可
        void saveImage(String parentPrefix, DataOutputStream out) throws IOException {
            String fullName = "";
            if (parent != null) {
                fullName = parentPrefix + "/" + name;
                new UTF8(fullName).write(out);
                if (blocks == null) {
                    out.writeInt(0);
                } else {
                    out.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        blocks[i].write(out);
                    }
                }
            }
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                // 下一级的prefix就是这一级的fullname
                child.saveImage(fullName, out);
            }
        }
    }

    INode rootDir = new INode("", null, null);
    TreeSet activeBlocks = new TreeSet();
    TreeMap activeLocks = new TreeMap();
    DataOutputStream editlog = null;
    boolean ready = false;

    /** Access an existing dfs name directory. */
    public FSDirectory(File dir) throws IOException {
        File fullimage = new File(dir, "image");
        // 不存在image文件夹，说明没有进行format操作，报错
        if (! fullimage.exists()) {
          throw new IOException("NameNode not formatted: " + dir);
        }
        File edits = new File(dir, "edits");
        // 装载完image和edits，成功后，重新保存一下
        if (loadFSImage(fullimage, edits)) {
            // 重新保存，目的是把edits文件删掉
            saveFSImage(fullimage, edits);
        }

        synchronized (this) {
            this.ready = true;
            this.notifyAll();
            // 打开editlog写出流，准备接受各种操作
            this.editlog = new DataOutputStream(new FileOutputStream(edits));
        }
    }

    /** Create a new dfs name directory.  Caution: this destroys all files
     * in this filesystem. */
    public static void format(File dir, Configuration conf)
      throws IOException {
        File image = new File(dir, "image");
        File edits = new File(dir, "edits");
        // image要么不存在，要么就删掉重建
        // edits要么不存在，要么就删掉
        // 如果以上无法满足，就报错
        if (!((!image.exists() || FileUtil.fullyDelete(image, conf)) &&
              (!edits.exists() || edits.delete()) &&
              image.mkdirs())) {
          
          throw new IOException("Unable to format: "+dir);
        }
    }

    /**
     * Shutdown the filestore
     */
    public void close() throws IOException {
        editlog.close();
    }

    /**
     * Block until the object is ready to be used.
     */
    void waitForReady() {
        if (! ready) {
            synchronized (this) {
                while (!ready) {
                    try {
                        this.wait(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }

    /**
     * Load in the filesystem image.  It's a big list of
     * filenames and blocks.  Return whether we should
     * "re-save" and consolidate the edit-logs
     */
    boolean loadFSImage(File fsdir, File edits) throws IOException {
        //
        // Atomic move sequence, to recover from interrupted save
        //
        File curFile = new File(fsdir, FS_IMAGE);
        File newFile = new File(fsdir, NEW_FS_IMAGE);
        File oldFile = new File(fsdir, OLD_FS_IMAGE);

        // Maybe we were interrupted between 2 and 4
        // 当前的，老的都存在，就删掉老的文件，并删掉edits
        if (oldFile.exists() && curFile.exists()) {
            oldFile.delete();
            if (edits.exists()) {
                edits.delete();
            }
            //新的，老的都存在，就把新的命名成当前的，删掉老的
        } else if (oldFile.exists() && newFile.exists()) {
            // Or maybe between 1 and 2
            newFile.renameTo(curFile);
            oldFile.delete();
            // 当前，新的都存在，删掉新的
        } else if (curFile.exists() && newFile.exists()) {
            // Or else before stage 1, in which case we lose the edits
            newFile.delete();
        }

        //
        // Load in bits
        //
        // 当前存在，就读进来
        if (curFile.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));
            try {
                // 读文件数目
                int numFiles = in.readInt();
                for (int i = 0; i < numFiles; i++) {
                    UTF8 name = new UTF8();
                    // 读文件名字
                    name.readFields(in);
                    // 读Block数量
                    int numBlocks = in.readInt();
                    if (numBlocks == 0) {
                        unprotectedAddFile(name, null);
                    } else {
                        Block blocks[] = new Block[numBlocks];
                        for (int j = 0; j < numBlocks; j++) {
                            blocks[j] = new Block();
                            // 把block信息读进来
                            blocks[j].readFields(in);
                        }
                        unprotectedAddFile(name, blocks);
                    }
                }
            } finally {
                in.close();
            }
        }
        // 有edits文件并且load成功了
        if (edits.exists() && loadFSEdits(edits) > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Load an edit log, and apply the changes to the in-memory structure
     *
     * This is where we apply edits that we've been writing to disk all
     * along.
     */
    int loadFSEdits(File edits) throws IOException {
        int numEdits = 0;

        if (edits.exists()) {
            // 读入edits
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(edits)));
            try {
                while (in.available() > 0) {
                    // 操作码
                    byte opcode = in.readByte();
                    numEdits++;
                    switch (opcode) {
                        //添加
                    case OP_ADD: {
                        UTF8 name = new UTF8();
                        name.readFields(in);
                        ArrayWritable aw = new ArrayWritable(Block.class);
                        aw.readFields(in);
                        Writable writables[] = (Writable[]) aw.get();
                        Block blocks[] = new Block[writables.length];
                        System.arraycopy(writables, 0, blocks, 0, blocks.length);
                        unprotectedAddFile(name, blocks);
                        break;
                    }
                    // 重命名
                    case OP_RENAME: {
                        UTF8 src = new UTF8();
                        UTF8 dst = new UTF8();
                        src.readFields(in);
                        dst.readFields(in);
                        unprotectedRenameTo(src, dst);
                        break;
                    }
                    // 删除
                    case OP_DELETE: {
                        UTF8 src = new UTF8();
                        // 读入文件名
                        src.readFields(in);
                        unprotectedDelete(src);
                        break;
                    }
                    // 建文件夹
                    case OP_MKDIR: {
                        UTF8 src = new UTF8();
                        // 读入文件夹
                        src.readFields(in);
                        // 在INode树里面加节点，前提是父亲INode需要事先存在，不然无法添加成功
                        unprotectedMkdir(src.toString());
                        break;
                    }
                    default: {
                        throw new IOException("Never seen opcode " + opcode);
                    }
                    }
                }
            } finally {
                in.close();
            }
        }
        return numEdits;
    }

    /**
     * Save the contents of the FS image
     */
    void saveFSImage(File fullimage, File edits) throws IOException {
        File curFile = new File(fullimage, FS_IMAGE);
        File newFile = new File(fullimage, NEW_FS_IMAGE);
        File oldFile = new File(fullimage, OLD_FS_IMAGE);

        //
        // Write out data
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
        try {
            // 除了根INode，其他所有节点数
            out.writeInt(rootDir.numItemsInTree() - 1);
            // 保存到image.new文件中
            rootDir.saveImage("", out);
        } finally {
            out.close();
        }
        // 4个步骤
        //
        // Atomic move sequence
        //
        // 1.  Move cur to old , 将当前文件image变为image.old
        curFile.renameTo(oldFile);

        // 2.  Move new to cur,  将image.new变为image
        newFile.renameTo(curFile);

        // 3.  Remove pending-edits file (it's been integrated with newFile)
        // 删掉edits文件，因为它们已经操作了INode，这些更改已经记录在新文件里了
        edits.delete();
        
        // 4.  Delete old   老的也不要了
        oldFile.delete();
    }

    /**
     * Write an operation to the edit log
     */
    void logEdit(byte op, Writable w1, Writable w2) {
        synchronized (editlog) {
            try {
                editlog.write(op);
                if (w1 != null) {
                    w1.write(editlog);
                }
                if (w2 != null) {
                    w2.write(editlog);
                }
            } catch (IOException ie) {
            }
        }
    }

    /**
     * Add the given filename to the fs.
     */
    public boolean addFile(UTF8 src, Block blocks[]) {
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree
        mkdirs(DFSFile.getDFSParent(src.toString()));
        if (unprotectedAddFile(src, blocks)) {
            logEdit(OP_ADD, src, new ArrayWritable(Block.class, blocks));
            return true;
        } else {
            return false;
        }
    }
    
    /**
     */
    boolean unprotectedAddFile(UTF8 name, Block blocks[]) {
        synchronized (rootDir) {
            if (blocks != null) {
                // Add file->block mapping
                for (int i = 0; i < blocks.length; i++) {
                    activeBlocks.add(blocks[i]);
                }
            }
            return (rootDir.addNode(name.toString(), blocks) != null);
        }
    }

    /**
     * Change the filename
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        waitForReady();
        if (unprotectedRenameTo(src, dst)) {
            logEdit(OP_RENAME, src, dst);
            return true;
        } else {
            return false;
        }
    }

    /**
     */
    boolean unprotectedRenameTo(UTF8 src, UTF8 dst) {
        synchronized(rootDir) {
            INode removedNode = rootDir.getNode(src.toString());
            if (removedNode == null) {
                return false;
            }
            removedNode.removeNode();
            // 如果是目录，就将src的文件名放到dst下
            if (isDir(dst)) {
                dst = new UTF8(dst.toString() + "/" + new File(src.toString()).getName());
            }
            INode newNode = rootDir.addNode(dst.toString(), removedNode.blocks);
            if (newNode != null) {
                // 把儿子们搬过去
                newNode.children = removedNode.children;
                // 修改儿子们的parent指针
                for (Iterator it = newNode.children.values().iterator(); it.hasNext(); ) {
                    INode child = (INode) it.next();
                    child.parent = newNode;
                }
                return true;
            } else {
                // 添加不进去，恢复成原样
                rootDir.addNode(src.toString(), removedNode.blocks);
                return false;
            }
        }
    }

    /**
     * Remove the file from management, return blocks
     */
    public Block[] delete(UTF8 src) {
        waitForReady();
        logEdit(OP_DELETE, src, null);
        return unprotectedDelete(src);
    }

    /**
     */
    Block[] unprotectedDelete(UTF8 src) {
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            // 没有相应节点，忽略掉
            if (targetNode == null) {
                return null;
            } else {
                //
                // Remove the node from the namespace and GC all
                // the blocks underneath the node.
                //
                // 找到了就把这个INode删掉，删除不成功就退出
                if (! targetNode.removeNode()) {
                    return null;
                } else {
                    // 删除成功了，把子树里的blocks全都拿来
                    Vector v = new Vector();
                    targetNode.collectSubtreeBlocks(v);
                    for (Iterator it = v.iterator(); it.hasNext(); ) {
                        Block b = (Block) it.next();
                        // 从活跃Blockset里面去除
                        activeBlocks.remove(b);
                    }
                    // 返回删掉的那些blocks
                    return (Block[]) v.toArray(new Block[v.size()]);
                }
            }
        }
    }

    /**
     */
    public int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders == null) {
            holders = new TreeSet();
            activeLocks.put(src, holders);
        }
        if (exclusive && holders.size() > 0) {
            return STILL_WAITING;
        } else {
            holders.add(holder);
            return COMPLETE_SUCCESS;
        }
    }

    /**
     */
    public int releaseLock(UTF8 src, UTF8 holder) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders != null && holders.contains(holder)) {
            holders.remove(holder);
            if (holders.size() == 0) {
                activeLocks.remove(src);
            }
            return COMPLETE_SUCCESS;
        } else {
            return OPERATION_FAILED;
        }
    }

    /**
     * Get a listing of files given path 'src'
     *
     * This function is admittedly very inefficient right now.  We'll
     * make it better later.
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        String srcs = normalizePath(src);

        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(srcs);
            if (targetNode == null) {
                return null;
            } else {
                Vector contents = new Vector();
                targetNode.listContents(contents);

                DFSFileInfo listing[] = new DFSFileInfo[contents.size()];
                int i = 0;
                for (Iterator it = contents.iterator(); it.hasNext(); i++) {
                    listing[i] = new DFSFileInfo( (INode) it.next() );
                }
                return listing;
            }
        }
    }

    /**
     * Get the blocks associated with the file
     */
    // 先得到文件名对应的inode，再返回其中的Blocks
    public Block[] getFile(UTF8 src) {
        waitForReady();
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                return targetNode.blocks;
            }
        }
    }

    /** 
     * Check whether the filepath could be created
     */
    public boolean isValidToCreate(UTF8 src) {
        String srcs = normalizePath(src);
        synchronized (rootDir) {
            if (srcs.startsWith("/") && 
                ! srcs.endsWith("/") && 
                rootDir.getNode(srcs) == null) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Check whether the path specifies a directory
     */
    public boolean isDir(UTF8 src) {
        synchronized (rootDir) {
            INode node = rootDir.getNode(normalizePath(src));
            return node != null && node.isDir();
        }
    }

    /**
     * Create the given directory and all its parent dirs.
     */
    public boolean mkdirs(UTF8 src) {
        return mkdirs(src.toString());
    }

    /**
     * Create directory entries for every item
     */
    boolean mkdirs(String src) {
        src = normalizePath(new UTF8(src));

        // Use this to collect all the dirs we need to construct
        Vector v = new Vector();

        // The dir itself
        v.add(src);

        // All its parents
        String parent = DFSFile.getDFSParent(src);
        while (parent != null) {
            v.add(parent);
            parent = DFSFile.getDFSParent(parent);
        }

        // Now go backwards through list of dirs, creating along
        // the way
        boolean lastSuccess = false;
        int numElts = v.size();
        for (int i = numElts - 1; i >= 0; i--) {
            String cur = (String) v.elementAt(i);
            INode inserted = unprotectedMkdir(cur);
            if (inserted != null) {
                logEdit(OP_MKDIR, new UTF8(inserted.computeName()), null);
                lastSuccess = true;
            } else {
                lastSuccess = false;
            }
        }
        return lastSuccess;
    }

    /**
     */
    INode unprotectedMkdir(String src) {
        synchronized (rootDir) {
            // block为null，添加的是文件夹
            return rootDir.addNode(src, null);
        }
    }

    /**
     */
    // 去掉路径里最后一个“/”
    String normalizePath(UTF8 src) {
        String srcs = src.toString();
        if (srcs.length() > 1 && srcs.endsWith("/")) {
            srcs = srcs.substring(0, srcs.length() - 1);
        }
        return srcs;
    }

    /**
     * Returns whether the given block is one pointed-to by a file.
     */
    // 是不是某个File里包含的Block
    public boolean isValidBlock(Block b) {
        synchronized (rootDir) {
            if (activeBlocks.contains(b)) {
                return true;
            } else {
                return false;
            }
        }
    }
}
