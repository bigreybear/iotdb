package org.apache.iotdb.db.metadata.research;

import org.apache.iotdb.db.metadata.artree.ArtTree;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/** Copy from {@link org.apache.iotdb.tsfile.read.TsFileSequenceReader} */
public class MockARTFileReader {
  private static final String FILE_PATH = "mtree_test".concat("_MockARTFile.mart");

  // use mmap to read/write file
  File file;
  FileChannel channel;
  ByteBuffer buffer;

  public MockARTFileReader() {}

  public MockARTFileReader(int pre, ArtTree tree) throws IOException {
    prepareFileWithPrefix(pre, tree);
    buffer = getReadOnlyBuffer();
  }

  public static void main(String[] args) throws IOException {
    System.out.println("hello");
    ArtTree tree = new ArtTree();
    Map<String, Long> answers = new HashMap<>();
    answers.put("root.sg1.d2.v2", 1L);
    answers.put("root.sg2.d3.v3", 2L);
    answers.put("root.sg2.d4.v1", 3L);
    answers.put("root.sg2.d3.v1", 4L);
    answers.put("root.sg2.xd3.xv1", 11L);
    answers.put("root.sg5.d1.v1", 5L);
    answers.put("root.sg5.d2.v1", 6L);
    answers.put("root.sg6.d1.v1", 7L);

    for (Map.Entry<String, Long> entry : answers.entrySet()) {
      tree.insert(entry.getKey().getBytes(), entry.getValue());
    }

    MockARTFileReader reader = new MockARTFileReader();
    reader.prepareFileWithPrefix(666, tree);

    for (Map.Entry<String, Long> entry : answers.entrySet()) {
      if (reader.getValue(entry.getKey()) != entry.getValue()) {
        System.exit(-1);
      }
    }
    System.out.println("ALL PASS");
    reader.close();

    // ByteBuffer buffer = reader.getReadOnlyBuffer();
    // ArtNode r2 = (ArtNode) ArtTree.deserialize(buffer);

    //    System.out.println(tree);

    // ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // serialize(((ArtNode)tree.root), baos);
    // ReadWriteIOUtils.write(tree.root.offset, baos);
    // System.out.println(baos.size());
    // byte[] res = baos.toByteArray();
    // traverse((ArtNode) tree.root, "");
    // System.out.println(tree.totalNodes());
    // System.out.println(tree.totalDepth());

    // ArtNode r2 = (ArtNode) deserialize(ByteBuffer.wrap(res));
    // traverseAfterDeserialize(r2, "");
    // tree.root = r2;
    // System.out.println(tree.totalNodes());
    // System.out.println(tree.totalDepth());
  }

  public long getValue(String key) throws IOException {
    if (buffer == null) {
      getReadOnlyBuffer();
    }

    buffer.position(buffer.capacity() - Long.BYTES);
    long nextPos = ReadWriteIOUtils.readLong(buffer);
    final byte[] keyBytes = key.getBytes();
    byte[] partKeyBytes;
    int idx = 0;
    byte type;

    buffer.position((int) nextPos);
    type = ReadWriteIOUtils.readByte(buffer);
    while (type != 0) {
      // look up until leaf
      partKeyBytes = ReadWriteIOUtils.readBool(buffer) ? readVarBytes(buffer) : null;

      if (partKeyBytes != null) {
        for (int i = 0; i < partKeyBytes.length; i++) {
          if (keyBytes[idx++] != partKeyBytes[i]) {
            // -1 for mismatch todo fuzzy lookup
            return -1L;
          }
        }
      }

      nextPos = type == 4 ? readFor256(keyBytes[idx++]) : readForArtNodes(keyBytes[idx++]);
      // mismatch // todo fuzzy lookup
      if (nextPos < 0) {
        return nextPos;
      }

      buffer.position((int) nextPos);
      type = ReadWriteIOUtils.readByte(buffer);
    }

    if (idx >= keyBytes.length) {
      if (readVarBytes(buffer) == null) {
        return ReadWriteIOUtils.readLong(buffer);
      }
      return -1L;
    }

    partKeyBytes = readVarBytes(buffer);
    // todo extracted as individual method
    if (partKeyBytes != null) {
      for (int i = 0; i < partKeyBytes.length; i++) {
        if (keyBytes[idx++] != partKeyBytes[i]) {
          // -1 for mismatch todo fuzzy lookup
          return -1L;
        }
      }
      return ReadWriteIOUtils.readLong(buffer);
    } else {
      return -1;
    }
  }

  private long readForArtNodes(byte tar) {
    int children_cnt = ReadWriteIOUtils.readInt(buffer);
    while (children_cnt-- > 0) {
      if (ReadWriteIOUtils.readByte(buffer) == tar) {
        return ReadWriteIOUtils.readLong(buffer);
      }
      ReadWriteIOUtils.readLong(buffer);
    }
    return -1L;
  }

  private long readFor256(byte tar) {
    int num = to_uint(tar);
    long res = ReadWriteIOUtils.readLong(buffer);
    while (num-- >= 0) {
      res = ReadWriteIOUtils.readLong(buffer);
    }
    return res;
  }

  static int to_uint(byte b) {
    return ((int) b) & 0xFF;
  }

  // NOTE specialize from ReadWriteIOUtils
  public static byte[] readVarBytes(ByteBuffer buffer) {
    int strLength = ReadWriteForEncodingUtils.readVarInt(buffer);
    if (strLength < 0) {
      return null;
    } else if (strLength == 0) {
      return null;
    }
    byte[] bytes = new byte[strLength];
    buffer.get(bytes, 0, strLength);
    return bytes;
  }

  private ByteBuffer getReadOnlyBuffer() throws IOException {
    file = new File(FILE_PATH);
    channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    if (buffer == null) {
      buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }
    return buffer;
  }

  public void close() throws IOException {
    channel.close();
  }

  private void prepareFileWithPrefix(int length, ArtTree tree) throws IOException {
    byte[] magicBytes = "MAGIC_BYTES".getBytes();
    int loop = (int) (length * 1.0 / magicBytes.length);

    file = new File(FILE_PATH);
    file.deleteOnExit();

    MockARTFileOutputStream martos = new MockARTFileOutputStream(new FileOutputStream(FILE_PATH));
    while (loop-- > 0) {
      martos.write(magicBytes);
    }

    tree.serializeToFile(martos);
    martos.close();
  }
}
