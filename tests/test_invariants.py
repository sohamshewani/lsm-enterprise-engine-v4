package com.enterprise.lsm.test;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive enterprise-grade LSM Tree Storage Engine and Test Suite.
 * Contains the complete engine implementation and concurrent stress/failure tests.
 */
public class LSMEnterpriseEngineTest {

    // =========================================================================
    // PRODUCTION ENGINE IMPLEMENTATION START
    // =========================================================================

    public static class LSMConfig {
        public int memTableSizeThreshold = 1024 * 1024; // 1MB
        public int sstTargetFileSize = 2 * 1024 * 1024; // 2MB
        public int maxLevels = 4;
        public double sizeRatio = 10.0;
        public boolean syncWrites = false;
    }

    public static class InternalKey implements Comparable<InternalKey> {
        private final byte[] userKey;
        private final long sequenceNumber;
        private final boolean isTombstone;

        public InternalKey(byte[] userKey, long sequenceNumber, boolean isTombstone) {
            this.userKey = userKey;
            this.sequenceNumber = sequenceNumber;
            this.isTombstone = isTombstone;
        }

        public byte[] getUserKey() { return userKey; }
        public long getSequenceNumber() { return sequenceNumber; }
        public boolean isTombstone() { return isTombstone; }

        @Override
        public int compareTo(InternalKey o) {
            int cmp = compareBytes(this.userKey, o.userKey);
            if (cmp != 0) return cmp;
            // Higher sequence number comes first (descending order for sequence)
            return Long.compare(o.sequenceNumber, this.sequenceNumber);
        }

        public static int compareBytes(byte[] a, byte[] b) {
            int len = Math.min(a.length, b.length);
            for (int i = 0; i < len; i++) {
                int res = Byte.compare(a[i], b[i]);
                if (res != 0) return res;
            }
            return Integer.compare(a.length, b.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InternalKey)) return false;
            InternalKey that = (InternalKey) o;
            return sequenceNumber == that.sequenceNumber &&
                    isTombstone == that.isTombstone &&
                    Arrays.equals(userKey, that.userKey);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(userKey);
            result = 31 * result + Long.hashCode(sequenceNumber);
            result = 31 * result + Boolean.hashCode(isTombstone);
            return result;
        }
    }

    public static class WriteBatch {
        private final List<Entry> entries = new ArrayList<>();

        public static class Entry {
            final byte[] key;
            final byte[] value;
            final boolean isTombstone;

            public Entry(byte[] key, byte[] value, boolean isTombstone) {
                this.key = key;
                this.value = value;
                this.isTombstone = isTombstone;
            }
        }

        public synchronized WriteBatch put(byte[] key, byte[] value) {
            entries.add(new Entry(key, value, false));
            return this;
        }

        public synchronized WriteBatch delete(byte[] key) {
            entries.add(new Entry(key, new byte[0], true));
            return this;
        }

        public synchronized List<Entry> getEntries() {
            return entries;
        }
    }

    public static class WAL implements Closeable {
        private final FileChannel channel;
        private final ReentrantLock lock = new ReentrantLock();

        public WAL(Path walPath) throws IOException {
            this.channel = FileChannel.open(walPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.APPEND);
        }

        public void append(long seq, byte[] key, byte[] value, boolean isTombstone, boolean sync) throws IOException {
            lock.lock();
            try {
                int payloadSize = 8 + 1 + 4 + key.length + 4 + (isTombstone ? 0 : value.length);
                ByteBuffer buf = ByteBuffer.allocate(4 + payloadSize);
                buf.putInt(payloadSize);
                buf.putLong(seq);
                buf.put(isTombstone ? (byte) 1 : (byte) 0);
                buf.putInt(key.length);
                buf.put(key);
                if (!isTombstone) {
                    buf.putInt(value.length);
                    buf.put(value);
                } else {
                    buf.putInt(0);
                }
                buf.flip();
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
                if (sync) {
                    channel.force(false);
                }
            } finally {
                lock.unlock();
            }
        }

        public List<ParsedWALEntry> recover() throws IOException {
            lock.lock();
            try {
                List<ParsedWALEntry> recovered = new ArrayList<>();
                channel.position(0);
                ByteBuffer lengthBuf = ByteBuffer.allocate(4);
                while (true) {
                    lengthBuf.clear();
                    int read = channel.read(lengthBuf);
                    if (read < 4) break;
                    lengthBuf.flip();
                    int payloadSize = lengthBuf.getInt();

                    ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize);
                    while (payloadBuf.hasRemaining()) {
                        if (channel.read(payloadBuf) == -1) {
                            // Truncated WAL
                            break;
                        }
                    }
                    if (payloadBuf.hasRemaining()) break; // Incomplete record
                    payloadBuf.flip();

                    long seq = payloadBuf.getLong();
                    boolean isTomb = payloadBuf.get() == 1;
                    int kLen = payloadBuf.getInt();
                    byte[] k = new byte[kLen];
                    payloadBuf.get(k);
                    int vLen = payloadBuf.getInt();
                    byte[] v = new byte[vLen];
                    if (!isTomb) {
                        payloadBuf.get(v);
                    }
                    recovered.add(new ParsedWALEntry(seq, k, v, isTomb));
                }
                return recovered;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() throws IOException {
            lock.lock();
            try {
                channel.close();
            } finally {
                lock.unlock();
            }
        }

        public static class ParsedWALEntry {
            public final long seq;
            public final byte[] key;
            public final byte[] value;
            public final boolean isTombstone;

            public ParsedWALEntry(long seq, byte[] key, byte[] value, boolean isTombstone) {
                this.seq = seq;
                this.key = key;
                this.value = value;
                this.isTombstone = isTombstone;
            }
        }
    }

    public static class SSTableWriter {
        public static void write(Path file, ConcurrentSkipListMap<InternalKey, byte[]> data) throws IOException {
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                // Format:
                // [Count]
                // For each entry: [Seq(8)][Tomb(1)][KeyLen(4)][Key][ValLen(4)][Value]
                // Index Block at the end: [IndexOffset(8)]
                
                long currentOffset = 0;
                List<IndexEntry> indexEntries = new ArrayList<>();

                ByteBuffer countBuf = ByteBuffer.allocate(8);
                countBuf.putLong(data.size());
                countBuf.flip();
                channel.write(countBuf);
                currentOffset += 8;

                for (Map.Entry<InternalKey, byte[]> entry : data.entrySet()) {
                    InternalKey ik = entry.getKey();
                    byte[] val = entry.getValue();
                    byte[] userKey = ik.getUserKey();

                    indexEntries.add(new IndexEntry(userKey, currentOffset));

                    int entrySize = 8 + 1 + 4 + userKey.length + 4 + (ik.isTombstone() ? 0 : val.length);
                    ByteBuffer buf = ByteBuffer.allocate(4 + entrySize);
                    buf.putInt(entrySize);
                    buf.putLong(ik.getSequenceNumber());
                    buf.put(ik.isTombstone() ? (byte) 1 : (byte) 0);
                    buf.putInt(userKey.length);
                    buf.put(userKey);
                    if (!ik.isTombstone()) {
                        buf.putInt(val.length);
                        buf.put(val);
                    } else {
                        buf.putInt(0);
                    }
                    buf.flip();
                    channel.write(buf);
                    currentOffset += 4 + entrySize;
                }

                // Write Index Block
                long indexBlockOffset = currentOffset;
                ByteBuffer indexCountBuf = ByteBuffer.allocate(4);
                indexCountBuf.putInt(indexEntries.size());
                indexCountBuf.flip();
                channel.write(indexCountBuf);
                currentOffset += 4;

                for (IndexEntry ie : indexEntries) {
                    ByteBuffer ieBuf = ByteBuffer.allocate(4 + ie.key.length + 8);
                    ieBuf.putInt(ie.key.length);
                    ieBuf.put(ie.key);
                    ieBuf.putLong(ie.offset);
                    ieBuf.flip();
                    channel.write(ieBuf);
                }

                // Footer: index block offset
                ByteBuffer footer = ByteBuffer.allocate(8);
                footer.putLong(indexBlockOffset);
                footer.flip();
                channel.write(footer);
            }
        }

        private static class IndexEntry {
            final byte[] key;
            final long offset;
            IndexEntry(byte[] key, long offset) {
                this.key = key;
                this.offset = offset;
            }
        }
    }

    public static class SSTableReader implements Closeable {
        private final Path file;
        private final FileChannel channel;
        private final long indexOffset;
        private final List<IndexEntry> index = new ArrayList<>();

        public static class IndexEntry {
            final byte[] key;
            final long offset;
            IndexEntry(byte[] key, long offset) {
                this.key = key;
                this.offset = offset;
            }
        }

        public SSTableReader(Path file) throws IOException {
            this.file = file;
            this.channel = FileChannel.open(file, StandardOpenOption.READ);
            
            // Read footer for index offset
            ByteBuffer footer = ByteBuffer.allocate(8);
            channel.position(channel.size() - 8);
            channel.read(footer);
            footer.flip();
            this.indexOffset = footer.getLong();

            // Load index
            channel.position(indexOffset);
            ByteBuffer countBuf = ByteBuffer.allocate(4);
            channel.read(countBuf);
            countBuf.flip();
            int count = countBuf.getInt();

            for (int i = 0; i < count; i++) {
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                channel.read(lenBuf);
                lenBuf.flip();
                int kLen = lenBuf.getInt();

                ByteBuffer kBuf = ByteBuffer.allocate(kLen + 8);
                channel.read(kBuf);
                kBuf.flip();
                byte[] k = new byte[kLen];
                kBuf.get(k);
                long offset = kBuf