package edu.uci.ics.tippers.common.exceptions;

/**
 * Created by peeyush on 26/3/17.
 */
public class CompilationException extends Exception {
    public CompilationException(String s) {
    }

    public CompilationException(AlgebricksException e) {
    }
    public CompilationException(Exception e) {
    }
}
