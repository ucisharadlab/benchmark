/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.parser;

import java.util.List;

public class ScopeChecker {

    protected static String quot = "\"";

    protected String eol = System.getProperty("line.separator", "\n");

    protected String[] inputLines;

    protected String defaultDataverse;

    private List<String> dataverses;
    private List<String> datasets;

    protected void setInput(String s) {
        inputLines = s.split("\n|\r\n?");
    }

    // Forbidden scopes are used to disallow, in a limit clause, variables
    // having the same name as a variable defined by the FLWOR in which that
    // limit clause appears.

    /**
     * Create a new scope, using the top scope in scopeStack as parent scope
     *
     * @return new scope
     */


    protected int appendExpected(StringBuilder expected, int[][] expectedTokenSequences, String[] tokenImage) {
        int maxSize = 0;
        for (int i = 0; i < expectedTokenSequences.length; i++) {
            if (maxSize < expectedTokenSequences[i].length) {
                maxSize = expectedTokenSequences[i].length;
            }
            for (int j = 0; j < expectedTokenSequences[i].length; j++) {
                append(expected, fixQuotes(tokenImage[expectedTokenSequences[i][j]]));
                append(expected, " ");
            }
            if (expectedTokenSequences[i][expectedTokenSequences[i].length - 1] != 0) {
                append(expected, "...");
            }
            append(expected, eol);
            append(expected, "    ");
        }
        return maxSize;
    }

    private void append(StringBuilder expected, String str) {
        if (expected != null) {
            expected.append(str);
        }
    }

    protected static String fixQuotes(String token) {
        final String stripped = stripQuotes(token);
        return stripped != null ? "'" + stripped + "'" : token;
    }

    protected static String stripQuotes(String token) {
        final int last = token.length() - 1;
        return token.charAt(0) == '"' && token.charAt(last) == '"' ? token.substring(1, last) : null;
    }

    protected static String addEscapes(String str) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            appendChar(escaped, str.charAt(i));
        }
        return escaped.toString();
    }

    private static void appendChar(StringBuilder escaped, char c) {
        char ch;
        switch (c) {
            case 0:
                return;
            case '\b':
                escaped.append("\\b");
                return;
            case '\t':
                escaped.append("\\t");
                return;
            case '\n':
                escaped.append("\\n");
                return;
            case '\f':
                escaped.append("\\f");
                return;
            case '\r':
                escaped.append("\\r");
                return;
            case '\"':
                escaped.append("\\\"");
                return;
            case '\'':
                escaped.append("\\\'");
                return;
            case '\\':
                escaped.append("\\\\");
                return;
            default:
                if ((ch = c) < 0x20 || ch > 0x7e) {
                    String s = "0000" + Integer.toString(ch, 16);
                    escaped.append("\\u").append(s.substring(s.length() - 4, s.length()));
                } else {
                    escaped.append(ch);
                }
        }
    }

    public static String removeQuotesAndEscapes(String s) {
        char q = s.charAt(0); // simple or double quote
        String stripped = s.substring(1, s.length() - 1);
        int pos = stripped.indexOf('\\');
        if (pos < 0) {
            return stripped;
        }
        StringBuilder res = new StringBuilder();
        int start = 0;
        while (pos >= 0) {
            res.append(stripped.substring(start, pos));
            char c = stripped.charAt(pos + 1);
            switch (c) {
                case '/':
                case '\\':
                    res.append(c);
                    break;
                case 'b':
                    res.append('\b');
                    break;
                case 'f':
                    res.append('\f');
                    break;
                case 'n':
                    res.append('\n');
                    break;
                case 'r':
                    res.append('\r');
                    break;
                case 't':
                    res.append('\t');
                    break;
                case '\'':
                case '"':
                    if (c == q) {
                        res.append(c);
                    }
                    break;
                default:
                    throw new IllegalStateException("'\\" + c + "' should have been caught by the lexer");
            }
            start = pos + 2;
            pos = stripped.indexOf('\\', start);
        }
        res.append(stripped.substring(start));
        return res.toString();
    }

    protected String getLine(int line) {
        return inputLines[line - 1];
    }

    protected String extractFragment(int beginLine, int beginColumn, int endLine, int endColumn) {
        StringBuilder extract = new StringBuilder();
        if (beginLine == endLine) {
            // special case that we need to handle separately
            return inputLines[beginLine - 1].substring(beginColumn, endColumn - 1).trim();
        }
        extract.append(inputLines[beginLine - 1].substring(beginColumn));
        for (int i = beginLine + 1; i < endLine; i++) {
            extract.append("\n");
            extract.append(inputLines[i - 1]);
        }
        extract.append("\n");
        extract.append(inputLines[endLine - 1].substring(0, endColumn - 1));
        return extract.toString().trim();
    }

    public void addDataverse(String dataverseName) {
        if (dataverses != null) {
            dataverses.add(dataverseName);
        }
    }

    public void addDataset(String datasetName) {
        if (datasets != null) {
            datasets.add(datasetName);
        }
    }

    public void setDataverses(List<String> dataverses) {
        this.dataverses = dataverses;
    }

    public void setDatasets(List<String> datasets) {
        this.datasets = datasets;
    }

    public List<String> getDataverses() {
        return dataverses;
    }

    public List<String> getDatasets() {
        return datasets;
    }
}
