
package org.apache.phoenix.compile;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.schema.TableRef;

public abstract class BaseMutationPlan implements MutationPlan {
    private final StatementContext context;
    private final Operation operation;
    
    public BaseMutationPlan(StatementContext context, Operation operation) {
        this.context = context;
        this.operation = operation;
    }
    
    @Override
    public Operation getOperation() {
        return operation;
    }
    
    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return context.getBindManager().getParameterMetaData();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        return ExplainPlan.EMPTY_PLAN;
    }

    @Override
    public TableRef getTargetRef() {
        return context.getCurrentTable();
    }
    
    @Override
    public Set<TableRef> getSourceRefs() {
        return Collections.emptySet();
    }

    @Override
    public Long getEstimatedRowsToScan() throws SQLException {
        return 0l;
    }

    @Override
    public Long getEstimatedBytesToScan() throws SQLException {
        return 0l;
    }

    @Override
    public Long getEstimateInfoTimestamp() throws SQLException {
        return 0l;
    }

    @Override
    public QueryPlan getQueryPlan() {
        return null;
    }

}
