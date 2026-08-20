using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Nostreets.Extensions.Core.Helpers.Data
{
    /// <summary>
    /// Combines predicate expressions while keeping them TRANSLATABLE.
    ///
    /// <para>
    /// 🔑 The obvious version does not work. <c>a =&gt; left(a) &amp;&amp; right(a)</c> compiles, but the
    /// tree it produces contains <c>Invoke</c> nodes, and EF cannot translate an invocation of one
    /// lambda from inside another — so the whole query silently falls back to an in-memory scan, which
    /// is the exact cost composing filters is meant to avoid. Splicing the BODIES together under one
    /// shared parameter is what keeps the result a plain <c>WHERE … AND …</c>.
    /// </para>
    ///
    /// <para>
    /// ⚠️ Two independently-written lambdas have DIFFERENT <see cref="ParameterExpression"/> instances
    /// even when both are named <c>a</c> — parameters match by reference, not by name. Splicing without
    /// rebinding produces a tree referencing a parameter that is not in scope, which throws at compile
    /// time rather than returning wrong rows. That is why the rewrite below exists.
    /// </para>
    /// </summary>
    public static class PredicateComposer
    {
        /// <summary>
        /// <paramref name="left"/> AND <paramref name="right"/>, as one expression over one parameter.
        /// A null operand is treated as "no constraint" and the other side is returned unchanged.
        /// </summary>
        public static Expression<Func<T, bool>> AndAlso<T>(Expression<Func<T, bool>> left,
                                                           Expression<Func<T, bool>> right)
        {
            if (left == null) return right;
            if (right == null) return left;

            var parameter = left.Parameters[0];
            var rebasedRight = new ParameterRebinder(right.Parameters[0], parameter).Visit(right.Body);

            return Expression.Lambda<Func<T, bool>>(
                Expression.AndAlso(left.Body, rebasedRight), parameter);
        }

        /// <summary>
        /// Folds every predicate together with AND. An empty or all-null sequence returns
        /// <paramref name="seed"/> unchanged, so a caller with no filters pays nothing.
        /// </summary>
        public static Expression<Func<T, bool>> AndAll<T>(Expression<Func<T, bool>> seed,
                                                          IEnumerable<Expression<Func<T, bool>>> predicates)
            => (predicates ?? Enumerable.Empty<Expression<Func<T, bool>>>())
                .Where(a => a != null)
                .Aggregate(seed, AndAlso);

        /// <summary>Points every reference to one parameter at another.</summary>
        private sealed class ParameterRebinder : ExpressionVisitor
        {
            private readonly ParameterExpression _from;
            private readonly ParameterExpression _to;

            public ParameterRebinder(ParameterExpression from, ParameterExpression to)
            {
                _from = from;
                _to = to;
            }

            protected override Expression VisitParameter(ParameterExpression node)
                => node == _from ? _to : base.VisitParameter(node);
        }
    }
}
