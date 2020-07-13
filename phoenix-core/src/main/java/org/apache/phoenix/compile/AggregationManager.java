e org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.visitor.SingleAggregateFunctionVisitor;

import com.google.common.collect.Sets;

/**
 * 
 * Class that manages aggregations during query compilation
 *
 * 
 * @since 0.1
 */
public class AggregationManager {
    private ClientAggregators aggregators;
    private int position = 0;
    
    public AggregationManager() {
    }

    public ClientAggregators getAggregators() {
        return aggregators;
    }
    
    public boolean isEmpty() {
        return aggregators == null || aggregators.getAggregatorCount() == 0;
    }
    
    /**
     * @return allocate the next available zero-based positional index
     * for the client-side aggregate function.
     */
    protected int nextPosition() {
        return position++;
    }
    
    public void setAggregators(ClientAggregators clientAggregator) {
        this.aggregators = clientAggregator;
    }
    /**
     * Compiles projection by:
     * 1) Adding RowCount aggregate function if not present when limiting rows. We need this
     *    to track how many rows have been scanned.
     * 2) Reordering aggregation functions (by putting fixed length aggregates first) to
     *    optimize the positional access of the aggregated value.
     */
    public void compile(StatementContext context, GroupByCompiler.GroupBy groupBy) throws
            SQLException {
        final Set<SingleAggregateFunction> aggFuncSet = Sets.newHashSetWithExpectedSize(context.getExpressionManager().getExpressionCount());

        Iterator<Expression> expressions = context.getExpressionManager().getExpressions();
        while (expressions.hasNext()) {
            Expression expression = expressions.next();
            expression.accept(new SingleAggregateFunctionVisitor() {
                @Override
                public Iterator<Expression> visitEnter(SingleAggregateFunction function) {
                    aggFuncSet.add(function);
                    return Collections.emptyIterator();
                }
            });
        }
        if (aggFuncSet.isEmpty() && groupBy.isEmpty()) {
            return;
        }
        List<SingleAggregateFunction> aggFuncs = new ArrayList<SingleAggregateFunction>(aggFuncSet);
        Collections.sort(aggFuncs, SingleAggregateFunction.SCHEMA_COMPARATOR);

        int minNullableIndex = getMinNullableIndex(aggFuncs,groupBy.isEmpty());
        context.getScan().setAttribute(BaseScannerRegionObserver.AGGREGATORS, ServerAggregators.serialize(aggFuncs, minNullableIndex));
        ClientAggregators clientAggregators = new ClientAggregators(aggFuncs, minNullableIndex);
        context.getAggregationManager().setAggregators(clientAggregators);
    }

    private static int getMinNullableIndex(List<SingleAggregateFunction> aggFuncs, boolean isUngroupedAggregation) {
        int minNullableIndex = aggFuncs.size();
        for (int i = 0; i < aggFuncs.size(); i++) {
            SingleAggregateFunction aggFunc = aggFuncs.get(i);
            if (isUngroupedAggregation ? aggFunc.getAggregator().isNullable() : aggFunc.getAggregatorExpression().isNullable()) {
                minNullableIndex = i;
                break;
            }
        }
        return minNullableIndex;
    }

}
