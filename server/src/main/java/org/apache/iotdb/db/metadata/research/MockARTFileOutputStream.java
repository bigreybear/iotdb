package org.apache.iotdb.db.metadata.research;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Copy from {@link org.apache.iotdb.tsfile.write.writer.LocalTsFileOutput} */
public class MockARTFileOutputStream extends OutputStream {

  private FileOutputStream outputStream;
  private BufferedOutputStream bufferedStream;
  private long position;

  public MockARTFileOutputStream(FileOutputStream outputStream) {
    this.outputStream = outputStream;
    this.bufferedStream = new BufferedOutputStream(outputStream);
    position = 0;
  }

  public synchronized void write(int b) throws IOException {
    bufferedStream.write(b);
    position++;
  }

  public synchronized void write(byte[] b) throws IOException {
    bufferedStream.write(b);
    position += b.length;
  }

  public synchronized void write(byte b) throws IOException {
    bufferedStream.write(b);
    position++;
  }

  public synchronized void write(byte[] buf, int start, int offset) throws IOException {
    bufferedStream.write(buf, start, offset);
    position += offset;
  }

  public synchronized void write(ByteBuffer b) throws IOException {
    bufferedStream.write(b.array());
    position += b.array().length;
  }

  public long getPosition() {
    return position;
  }

  public void close() throws IOException {
    bufferedStream.close();
    outputStream.close();
  }

  public OutputStream wrapAsStream() {
    return this;
  }

  public void flush() throws IOException {
    this.bufferedStream.flush();
  }

  public void truncate(long size) throws IOException {
    bufferedStream.flush();
    outputStream.getChannel().truncate(size);
    position = outputStream.getChannel().position();
  }
}
