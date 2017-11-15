/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.expressions;

/**
 * Point to range over byte array
 */
public interface IPointable extends IValueReference {
    /**
     * Point to the range from position = start with length = length over the byte array bytes
     *
     * @param bytes
     *            the byte array
     * @param start
     *            the start offset
     * @param length
     *            the length of the range
     */
    void set(byte[] bytes, int start, int length);

    /**
     * Point to the same range pointed to by the passed pointer
     *
     * @param pointer
     *            the pointer to the targetted range
     */
    void set(IValueReference pointer);
}
