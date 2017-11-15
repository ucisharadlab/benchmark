/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.functions;

public interface IFunctionInfo {
    FunctionIdentifier getFunctionIdentifier();

    public boolean isFunctional();
}
