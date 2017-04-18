package com.baidu.hugegraph.backend;

/**
 * Created by jishilei on 17/3/19.
 */
public interface Transaction {



    /**
     * Commits the transaction and persists all modifications to the backend.
     * <p>
     * Call either this method or {@link #rollback()} at most once per instance.
     *
     * @throws BackendException
     */
    public void commit() throws BackendException;

    /**
     * Aborts (or rolls back) the transaction.
     * <p>
     * Call either this method or {@link #commit()} at most once per instance.
     *
     * @throws BackendException
     */
    public void rollback() throws BackendException;
}
