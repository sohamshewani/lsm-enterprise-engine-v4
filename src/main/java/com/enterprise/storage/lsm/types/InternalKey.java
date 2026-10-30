package com.enterprise.storage.lsm.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * InternalKey represents the composite binary structure used internally by the LSM engine.
 * It combines a user-defined key byte array, a 64-bit monotonically increasing sequence number 
 * (providing MVCC and snapshot isolation support), and an 8-bit value type flag (PUT vs DELETE).
 *
 * <p>Binary Layout on disk/memory:
 * <ul>
 *   <li>[0 ... userKey.length - 1]: User Key bytes</li>
 *   <li>[userKey.length]: 8-byte (64-bit) Sequence Number (Big-Endian format)</li>
 *   <li>[userKey.length + 8]: 1-byte (8-bit) Value Type Flag</li>
 * </ul>
 *
 * <p>Sorting Invariant:
 * Sorted lexicographically by user key ascending, then sequence number descending 
 * (meaning newer mutations with higher sequence numbers appear before older mutations with 
 * lower sequence numbers for the same user key).
 */
public final class InternalKey implements Comparable<InternalKey> {

    public static final byte TYPE_DELETE = 0x00;
    public static final byte TYPE_PUT = 0x01;

    private final byte[] userKey;
    private final long sequenceNumber;
    private final byte valueType;

    /**
     * Constructs a new InternalKey.
     *
     * @     the raw user-defined key bytes (defensively copied)
     * @param sequenceNumber the 64-bit MVCC sequence number
     * @param valueType      the operation type ({@link #TYPE_PUT} or {@link #TYPE_DELETE})
     */
    public InternalKey(byte[] userKey, long sequenceNumber, byte valueType) {
        Objects.requireNonNull(userKey, "userKey cannot be null");
        if (valueType != TYPE_PUT && valueType != TYPE_DELETE) {
            throw new IllegalArgumentException("Invalid valueType: " + valueType);
        }
        this.userKey = Arrays.copyOf(userKey, userKey.length);
        this.sequenceNumber = sequenceNumber;
        this.valueType = valueType;
    }

    /**
     * Returns a defensive copy of the user key bytes.
     *
     * @return a new byte array containing the user key
     */
    public byte[] getUserKey() {
        return Arrays.copyOf(userKey, userKey.length);
    }

    /**
     * Returns the raw user key reference without copying. 
     * Use with caution in performance-critical paths where mutation is forbidden.
     *
     * @return the internal user key byte array
     */
    public byte[] getUserKeyUnsafe() {
        return userKey;
    }

    /**
     * Returns the 64-bit MVCC sequence number.
     *
     * @return sequence number
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Returns the 8-bit value type flag ({@link #TYPE_PUT} or {@link #TYPE_DELETE}).
     *
     * @return value type flag
     */
    public byte getValueType() {
        return valueType;
    }

    /**
     * Calculates the total encoded byte length of this InternalKey.
     *
     * @return total size in bytes
     */
    public int serializedSize() {
        return userKey.length + Long.BYTES + Byte.BYTES;
    }

    /**
     * Serializes this InternalKey into the provided ByteBuffer.
     *
     * @        the target ByteBuffer
     * @throws IllegalArgumentException if the buffer lacks sufficient remaining capacity
     */
    public void encode(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        int required = serializedSize();
        if (buffer.remaining() < required) {
            throw new IllegalArgumentException("Buffer remaining capacity " + buffer.remaining() 
                + " is less than required size " + required);
        }
        buffer.put(userKey);
        buffer.putLong(sequenceNumber);
        buffer.put(valueType);
    }

    /**
     * Serializes this InternalKey into a newly allocated byte array.
     *
     * @return a newly allocated byte array containing the encoded key
     */
    public byte[] encode() {
        byte[] data = new byte[serializedSize()];
        ByteBuffer buf = ByteBuffer.wrap(data);
        encode(buf);
        return data;
    }

    /**
     * Decodes an InternalKey from a ByteBuffer given the known user key length.
     * The ByteBuffer position is advanced by the total size of the InternalKey.
     *
     * @        the source ByteBuffer
     * @param userKeyLength  the exact length of the embedded user key
     * @return a fully populated InternalKey instance
     */
    public static InternalKey decode(ByteBuffer buffer, int userKeyLength) {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        if (userKeyLength < 0) {
            throw new IllegalArgumentException("userKeyLength cannot be negative: " + userKeyLength);
        }
        int totalRequired = userKeyLength + Long.BYTES + Byte.BYTES;
        if (buffer.remaining() < totalRequired) {
            throw new IllegalArgumentException("Buffer remaining capacity " + buffer.remaining() 
                + " is less than required size " + totalRequired);
        }

        byte[] uKey = new byte[userKeyLength];
        buffer.get(uKey);
        long seq = buffer.getLong();
        byte vType = buffer.get();

        return new InternalKey(uKey, seq, vType);
    }

    /**
     * Compares this InternalKey with another InternalKey according to system sorting invariants:
     * 1. Lexicographically by user key ascending.
     * 2. If user keys are identical, by sequence number descending (higher sequence numbers come first).
     *
     * @param other the other InternalKey to compare against
     * @return negative if this < other, zero if equal, positive if this > other
     */
    @Override
    public int compareTo(InternalKey other) {
        Objects.requireNonNull(other, "other InternalKey cannot be null");
        
        int cmp = compareUserKeys(this.userKey, other.userKey);
        if (cmp != 0) {
            return cmp;
        }

        // Sequence number descending: higher sequence number comes first (is "smaller" in LSM sorting order)
        return Long.compare(other.sequenceNumber, this.sequenceNumber);
    }

    /**
     * Utility method to lexicographically compare two byte arrays unsigned.
     */
    public static int compareUserKeys(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int bi1 = a[i] & 0xFF;
            int bi2 = b[i] & 0xFF;
            if (bi1 != bi2) {
                return bi1 - bi2;
            }
        }
        return a.length - b.length;
    }

    /**
     * Compares only the user key portion of this InternalKey against another user key.
     *
     * @        the user key bytes to compare
     * @return comparison result
     */
    public int compareUserKeyOnly(byte[] otherUserKey) {
        Objects.requireNonNull(otherUserKey, "otherUserKey cannot be null");
        return compareUserKeys(this.userKey, otherUserKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InternalKey)) return false;
        InternalKey that = (InternalKey) o;
        return sequenceNumber == that.sequenceNumber 
                && valueType == that.valueType 
                && Arrays.equals(userKey, that.userKey);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(userKey);
        result = 31 * result + Long.hashCode(sequenceNumber);
        result = 31 * result + (int) valueType;
        return result;
    }

    @Override
    public String toString() {
        return "InternalKey{" +
                "userKeyBytes=" + userKey.length +
                ", sequenceNumber=" + sequenceNumber +
                ", valueType=" + (valueType == TYPE_PUT ? "PUT" : "DELETE") +
                '}';
    }
}