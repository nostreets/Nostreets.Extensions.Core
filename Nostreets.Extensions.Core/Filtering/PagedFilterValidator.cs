using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Nostreets.Extensions.Core.Filtering
{
    /// <summary>
    /// Strict default-deny validator for an inbound, untrusted filter expression, run BEFORE it is compiled
    /// and invoked server-side. Walks the tree and throws <see cref="PagedFilterValidationException"/> on ANY
    /// node not on the allow-list — the gate against arbitrary-code execution from a crafted serialized
    /// expression (Serialize.Linq can reconstruct arbitrary trees; this is what makes that safe).
    ///
    /// Allows ONLY: the entity parameter; property/field access rooted at that parameter; constants;
    /// comparison / logical / boolean operators; <c>Not</c> and built-in <c>Convert</c>; and a small set of
    /// safe <see cref="string"/> methods. Everything else — arbitrary method calls, member access off a
    /// non-parameter target, object construction, type tests, indexers, etc. — is rejected.
    /// </summary>
    public sealed class PagedFilterValidator : ExpressionVisitor
    {
        private static readonly HashSet<string> AllowedStringInstanceMethods = new(StringComparer.Ordinal)
        {
            nameof(string.Contains), nameof(string.StartsWith), nameof(string.EndsWith),
            nameof(string.Equals), nameof(string.ToLower), nameof(string.ToUpper), nameof(string.Trim)
        };

        private static readonly HashSet<string> AllowedStringStaticMethods = new(StringComparer.Ordinal)
        {
            nameof(string.IsNullOrEmpty), nameof(string.IsNullOrWhiteSpace)
        };

        private PagedFilterValidator() { }

        /// <summary>
        /// Validates <paramref name="lambda"/>; throws <see cref="PagedFilterValidationException"/> on the
        /// first disallowed node. The caller is expected to have already confirmed the lambda is a
        /// single-parameter <c>Func&lt;T,bool&gt;</c>.
        /// </summary>
        public static void Validate(LambdaExpression lambda)
        {
            if (lambda == null) throw new PagedFilterValidationException("Filter expression is null.");
            new PagedFilterValidator().Visit(lambda);
        }

        public override Expression? Visit(Expression? node)
        {
            if (node == null) return null;

            switch (node.NodeType)
            {
                // structure + leaves
                case ExpressionType.Lambda:
                case ExpressionType.Parameter:
                case ExpressionType.Constant:
                case ExpressionType.MemberAccess: // further constrained in VisitMember
                case ExpressionType.Call:         // further constrained in VisitMethodCall
                // comparisons
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                // logical / boolean
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.And:
                case ExpressionType.Or:
                case ExpressionType.Not:
                // safe conversions (nullable lifts, enum <-> underlying) — user-defined ones rejected in VisitUnary
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return base.Visit(node);

                default:
                    throw new PagedFilterValidationException(
                        $"Filter contains a disallowed expression node '{node.NodeType}'.");
            }
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Allow only property/field chains rooted at the entity parameter — no static members, and no
            // access off an arbitrary object (e.g. a leftover closure constant).
            if (!IsParameterRooted(node))
                throw new PagedFilterValidationException(
                    $"Filter member access '{node.Member.Name}' must read a property/field of the entity parameter.");

            return base.VisitMember(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var declaring = node.Method.DeclaringType;

            bool allowed = declaring == typeof(string) &&
                ((!node.Method.IsStatic && AllowedStringInstanceMethods.Contains(node.Method.Name)) ||
                 (node.Method.IsStatic && AllowedStringStaticMethods.Contains(node.Method.Name)));

            if (!allowed)
                throw new PagedFilterValidationException(
                    $"Filter calls a disallowed method '{declaring?.FullName}.{node.Method.Name}'.");

            return base.VisitMethodCall(node); // recurse into Object + Arguments (each re-checked by Visit)
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // A Convert carrying a Method is a user-defined conversion operator — it runs code, so reject it.
            if ((node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
                && node.Method != null)
                throw new PagedFilterValidationException(
                    "Filter uses a user-defined conversion operator, which is not allowed.");

            return base.VisitUnary(node);
        }

        private static bool IsParameterRooted(Expression? expression)
        {
            // Walk down a member-access chain; the root must be the lambda's parameter.
            while (expression is MemberExpression member)
                expression = member.Expression;

            return expression is ParameterExpression;
        }
    }
}
