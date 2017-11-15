package edu.uci.ics.tippers.tql.parser;

/**
 * Created by peeyush on 7/5/17.
 */
public class TQLParserFactory {

    public TQLParser createParser(String query) {
        return new TQLParser(query);
    }

}
