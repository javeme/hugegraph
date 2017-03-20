package com.baidu.hugegraph2.backend.store;


import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.query.KeyQuery;
import com.baidu.hugegraph2.backend.query.SliceQuery;

import java.util.List;


/**
 * Created by jishilei on 17/3/19.
 */
public interface DBStore {

    public void mutate(List<DBEntry> additions, List<Object> deletions, StoreTransaction tx);

    //public DBEntry getEntry(KeyQuery query, StoreTransaction txh) throws BackendException ;

    //public List<DBEntry> getSlice(List<Object> keys, SliceQuery query, StoreTransaction txh) throws BackendException ;

    public String getName();

    /**
     * Closes this store
     *
     * @throws BackendException
     */
    public void close() throws BackendException;

    public void clear();

}
