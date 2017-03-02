// Copyright 2017 hugegraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph.hadoop.formats.util.input;

import com.baidu.hugegraph.diskstorage.keycolumnvalue.SliceQuery;
import com.baidu.hugegraph.graphdb.database.RelationReader;
import com.baidu.hugegraph.graphdb.idmanagement.IDManager;
import com.baidu.hugegraph.graphdb.types.TypeInspector;

/**
 */
public interface HugeGraphHadoopSetup {

    public TypeInspector getTypeInspector();

    public SystemTypeInspector getSystemTypeInspector();

    public RelationReader getRelationReader(long vertexid);

    public IDManager getIDManager();

    /**
     * Return an input slice across the entire row.
     *
     * TODO This would ideally slice only columns inside the row needed by the query.
     * The slice must include the hidden vertex state property (to filter removed vertices).
     *
     */
    public SliceQuery inputSlice();

    public void close();

    public boolean getFilterPartitionedVertices();

}
