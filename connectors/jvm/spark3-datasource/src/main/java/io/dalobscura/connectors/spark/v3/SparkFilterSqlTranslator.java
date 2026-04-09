package io.dalobscura.connectors.spark.v3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;

public final class SparkFilterSqlTranslator {
    public SparkFilterTranslation translate(Filter[] filters) {
        List<String> pushed = new ArrayList<>();
        List<Filter> residual = new ArrayList<>();
        for (Filter filter : filters) {
            TranslationPiece piece = translateFilter(filter);
            piece.pushed().ifPresent(pushed::add);
            residual.addAll(piece.residual());
        }

        Optional<String> pushedSql = joinConjuncts(pushed);
        return new SparkFilterTranslation(pushedSql, residual.toArray(new Filter[0]));
    }

    private TranslationPiece translateFilter(Filter filter) {
        if (filter instanceof And) {
            And and = (And) filter;
            TranslationPiece left = translateFilter(and.left());
            TranslationPiece right = translateFilter(and.right());
            List<String> conjuncts = new ArrayList<>();
            left.pushed().ifPresent(conjuncts::add);
            right.pushed().ifPresent(conjuncts::add);
            List<Filter> residual = new ArrayList<>();
            residual.addAll(left.residual());
            residual.addAll(right.residual());
            return new TranslationPiece(joinConjuncts(conjuncts), residual);
        }

        if (filter instanceof Or) {
            Or or = (Or) filter;
            TranslationPiece left = translateFilter(or.left());
            TranslationPiece right = translateFilter(or.right());
            if (left.pushed().isPresent()
                    && right.pushed().isPresent()
                    && left.residual().isEmpty()
                    && right.residual().isEmpty()) {
                return new TranslationPiece(
                        Optional.of("(" + left.pushed().get() + ") OR (" + right.pushed().get() + ")"),
                        List.of());
            }
            return new TranslationPiece(Optional.empty(), List.of(filter));
        }

        if (filter instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) filter;
            return new TranslationPiece(
                    Optional.of(equalTo.attribute() + " = " + literal(equalTo.value())),
                    List.of());
        }

        if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            return new TranslationPiece(
                    Optional.of(gt.attribute() + " > " + literal(gt.value())),
                    List.of());
        }

        if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gte = (GreaterThanOrEqual) filter;
            return new TranslationPiece(
                    Optional.of(gte.attribute() + " >= " + literal(gte.value())),
                    List.of());
        }

        if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            return new TranslationPiece(
                    Optional.of(lt.attribute() + " < " + literal(lt.value())),
                    List.of());
        }

        if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lte = (LessThanOrEqual) filter;
            return new TranslationPiece(
                    Optional.of(lte.attribute() + " <= " + literal(lte.value())),
                    List.of());
        }

        if (filter instanceof In) {
            In in = (In) filter;
            if (in.values().length == 0) {
                return new TranslationPiece(Optional.empty(), List.of(filter));
            }
            String values =
                    Arrays.stream(in.values())
                            .map(this::literal)
                            .collect(Collectors.joining(", "));
            return new TranslationPiece(
                    Optional.of(in.attribute() + " IN (" + values + ")"),
                    List.of());
        }

        if (filter instanceof IsNull) {
            IsNull isNull = (IsNull) filter;
            return new TranslationPiece(Optional.of(isNull.attribute() + " IS NULL"), List.of());
        }

        if (filter instanceof IsNotNull) {
            IsNotNull isNotNull = (IsNotNull) filter;
            return new TranslationPiece(
                    Optional.of(isNotNull.attribute() + " IS NOT NULL"),
                    List.of());
        }

        return new TranslationPiece(Optional.empty(), List.of(filter));
    }

    private String literal(Object value) {
        if (value instanceof String) {
            String s = (String) value;
            return "'" + s.replace("'", "''") + "'";
        }
        return String.valueOf(value);
    }

    private Optional<String> joinConjuncts(List<String> conjuncts) {
        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                conjuncts.stream()
                        .map(this::groupForConjunction)
                        .collect(Collectors.joining(" AND ")));
    }

    private String groupForConjunction(String sql) {
        return hasTopLevelOr(sql) ? "(" + sql + ")" : sql;
    }

    private boolean hasTopLevelOr(String sql) {
        int depth = 0;
        for (int index = 0; index < sql.length(); index++) {
            char current = sql.charAt(index);
            if (current == '(') {
                depth++;
                continue;
            }
            if (current == ')') {
                depth = Math.max(0, depth - 1);
                continue;
            }
            if (depth == 0 && sql.startsWith(" OR ", index)) {
                return true;
            }
        }
        return false;
    }

    private static final class TranslationPiece {
        private final Optional<String> pushed;
        private final List<Filter> residual;

        private TranslationPiece(Optional<String> pushed, List<Filter> residual) {
            this.pushed = pushed;
            this.residual = residual;
        }

        private Optional<String> pushed() {
            return pushed;
        }

        private List<Filter> residual() {
            return residual;
        }
    }
}
