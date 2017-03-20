package com.baidu.hugegraph2.backend;

/**
 * Created by jishilei on 17/3/19.
 */
public class BackendException extends Exception {

    /**
     * @param msg Exception message
     */
    public BackendException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public BackendException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public BackendException(Throwable cause) {
        this("Exception in backend backend.", cause);
    }
}
