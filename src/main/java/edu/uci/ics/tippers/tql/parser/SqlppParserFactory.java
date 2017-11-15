/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.parser;


public class SqlppParserFactory {

    public SQLPPParser createParser(String query) {
        return new SQLPPParser(query);
    }

}
