/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.util.metadata.entities;

import java.util.List;

public class Function {
    private static final long serialVersionUID = 1L;
    public static final String LANGUAGE_AQL = "AQL";
    public static final String LANGUAGE_JAVA = "JAVA";

    public static final String RETURNTYPE_VOID = "VOID";
    public static final String NOT_APPLICABLE = "N/A";

    private final String dataverse;
    private final String name;
    private final int arity;
    private final List<String> params;
    private final String body;
    private final String returnType;
    private final String language;
    private final String kind;

    public Function(String dataverseName, String functionName, int arity, List<String> params, String returnType,
                    String functionBody, String language, String functionKind) {
        this.dataverse = dataverseName;
        this.name = functionName;
        this.params = params;
        this.body = functionBody;
        this.returnType = returnType == null ? RETURNTYPE_VOID : returnType;
        this.language = language;
        this.kind = functionKind;
        this.arity = arity;
    }

    public String getDataverseName() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

    public List<String> getParams() {
        return params;
    }

    public String getFunctionBody() {
        return body;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public int getArity() {
        return arity;
    }

    public String getKind() {
        return kind;
    }

}
