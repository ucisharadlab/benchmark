/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.expressions;

public interface IValueReference {
    byte[] getByteArray();

    int getStartOffset();

    int getLength();
}
