package edu.uci.ics.tippers.wrapper.griddb.translator;

import com.toshiba.mwcloud.gs.GSException;
import edu.uci.ics.tippers.common.exceptions.CompilationException;
import edu.uci.ics.tippers.tql.lang.common.base.Statement;
import edu.uci.ics.tippers.tql.lang.common.statement.Query;
import edu.uci.ics.tippers.tql.lang.tql.NamedCollection;
import edu.uci.ics.tippers.tql.lang.tql.SensorToObservation;
import edu.uci.ics.tippers.tql.parser.TQLParser;
import edu.uci.ics.tippers.tql.parser.TQLParserFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peeyush on 11/5/17.
 */
public class TQLTranslator {

    private List<Statement> statementList = new ArrayList<Statement>();
    private Map<String, JSONArray> namedCollections = new HashMap<>();

    public TQLTranslator(String query) throws CompilationException {

        TQLParserFactory parserFactory = new TQLParserFactory();
        TQLParser parser = parserFactory.createParser(query);
        statementList = parser.parse();
    }

    public JSONArray getData() throws CompilationException, GSException, JSONException {

        Translator translator = null;
        for(Statement statement : statementList) {
            if (statement.getKind() == Statement.Kind.DECLARATION) {
                ;
            }
        }

        for(Statement statement : statementList) {
            if (statement.getKind() == Statement.Kind.NAMEDCOLLECTION) {
                NamedCollection namedCollection = (NamedCollection)statement;
                Query query = namedCollection.getQuery();
                translator = new Translator(query, namedCollections);
                namedCollections.put(namedCollection.getIdentifer(), translator.getData());
            }
        }

        for(Statement statement : statementList) {
            if (statement.getKind() == Statement.Kind.SENOSR_TO_COLLECTION) {
                SensorToObservation senToObs = (SensorToObservation) statement;
                namedCollections.put(senToObs.getObservationIdentifier(),
                        translator.fetchObservationFromSensor(namedCollections.get(
                                senToObs.getSensorIdentifier()
                        )));
            }
        }

        for(Statement statement : statementList) {
            if (statement.getKind() == Statement.Kind.QUERY) {
                Query query = (Query)statement;
                translator = new Translator(query, namedCollections);
                return translator.getData();
            }
        }
        return null;
    }
}
