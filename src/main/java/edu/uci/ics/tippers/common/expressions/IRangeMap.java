/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.expressions;


public interface IRangeMap {
    public IPointable getFieldSplit(int columnIndex, int splitIndex);

    public int getSplitCount();

    public byte[] getByteArray(int columnIndex, int splitIndex);

    public int getStartOffset(int columnIndex, int splitIndex);

    public int getLength(int columnIndex, int splitIndex);

    public int getTag(int columnIndex, int splitIndex);
}
