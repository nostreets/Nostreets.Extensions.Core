using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Nostreets.Extensions.Helpers.Data.QueryProvider; // PartialEvaluator

using Serialize.Linq.Serializers;

namespace Nostreets.Extensions.Core.Filtering
{
    /// <summary>
    /// Serializes / deserializes the predicate filters of <c>PagedListRequest&lt;T&gt;</c> for transport over
    /// HTTP using Serialize.Linq.
    ///
    /// Outbound (client): closures are folded to constants via <see cref="PartialEvaluator"/> (so a filter that
    /// captures a local variable — <c>x =&gt; x.Name == localVar</c> — serializes the VALUE, not a closure
    /// member access that the server allow-list would reject), then the expression is text-serialized.
    ///
    /// Inbound (server): each string is deserialized, run through the strict <see cref="PagedFilterValidator"/>
    /// allow-list, and ONLY then compiled — an unvalidated tree is never compiled or invoked. This is the gate
    /// that makes accepting a serialized expression over the wire safe (it would otherwise be an RCE vector).
    /// </summary>
    public static class PagedFilterCodec
    {
        private static ExpressionSerializer CreateSerializer() => new ExpressionSerializer(new JsonSerializer());

        /// <summary>Folds closures to constants and serializes a predicate into a transportable string.</summary>
        public static string Serialize<T>(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));

            var reduced = PartialEvaluator.Eval(filter); // captured variables -> ConstantExpression
            return CreateSerializer().SerializeText(reduced);
        }

        /// <summary>
        /// Deserializes, VALIDATES (strict allow-list), and compiles each serialized predicate into a
        /// <c>Func&lt;T,bool&gt;</c>. Throws <see cref="PagedFilterValidationException"/> if any entry is
        /// malformed, is not a single-parameter predicate over <typeparamref name="T"/>, or contains a
        /// disallowed node — in which case nothing is compiled or invoked.
        /// </summary>
        /// <summary>
        /// Same deserialize-and-validate as <see cref="CompileValidated{T}"/>, but hands back the
        /// EXPRESSION TREES instead of compiled delegates.
        /// <para>
        /// 🔑 Why both exist: a compiled <c>Func</c> can never be translated to SQL, and compiling is
        /// irreversible - the tree is gone. A paged read that composes its filters from
        /// <see cref="CompileValidated{T}"/> is therefore forced to evaluate IN MEMORY, which means
        /// loading the whole table to return one page. Composing from these keeps the predicate
        /// translatable end to end.
        /// </para>
        /// <para>
        /// ⚠️ The validation is identical and is NOT optional - these expressions arrive over the wire,
        /// so <c>PagedFilterValidator</c>'s allow-list is what stops a caller sending an arbitrary
        /// expression tree for the server to evaluate. Returning trees rather than delegates does not
        /// relax that; it only defers compilation to EF, or to the caller if it has to fall back.
        /// </para>
        /// </summary>
        public static List<Expression<Func<T, bool>>> DeserializeValidated<T>(IEnumerable<string> serializedFilters)
        {
            var result = new List<Expression<Func<T, bool>>>();
            if (serializedFilters == null) return result;

            var serializer = CreateSerializer();

            foreach (var serialized in serializedFilters)
            {
                if (string.IsNullOrWhiteSpace(serialized)) continue;

                Expression expression;
                try
                {
                    expression = serializer.DeserializeText(serialized);
                }
                catch (Exception ex)
                {
                    throw new PagedFilterValidationException("A filter expression could not be deserialized.", ex);
                }

                if (expression is not LambdaExpression lambda
                    || lambda.Parameters.Count != 1
                    || lambda.Parameters[0].Type != typeof(T)
                    || lambda.ReturnType != typeof(bool))
                {
                    throw new PagedFilterValidationException(
                        $"A filter expression is not a single-parameter predicate over '{typeof(T).Name}'.");
                }

                PagedFilterValidator.Validate(lambda); // throws on a disallowed node

                result.Add((Expression<Func<T, bool>>)lambda);
            }

            return result;
        }

        public static List<Func<T, bool>> CompileValidated<T>(IEnumerable<string> serializedFilters)
        {
            var result = new List<Func<T, bool>>();
            if (serializedFilters == null) return result;

            var serializer = CreateSerializer();

            foreach (var serialized in serializedFilters)
            {
                if (string.IsNullOrWhiteSpace(serialized)) continue;

                Expression expression;
                try
                {
                    expression = serializer.DeserializeText(serialized);
                }
                catch (Exception ex)
                {
                    throw new PagedFilterValidationException("A filter expression could not be deserialized.", ex);
                }

                if (expression is not LambdaExpression lambda
                    || lambda.Parameters.Count != 1
                    || lambda.Parameters[0].Type != typeof(T)
                    || lambda.ReturnType != typeof(bool))
                {
                    throw new PagedFilterValidationException(
                        $"A filter expression is not a single-parameter predicate over '{typeof(T).Name}'.");
                }

                PagedFilterValidator.Validate(lambda); // throws on a disallowed node

                result.Add(((Expression<Func<T, bool>>)lambda).Compile());
            }

            return result;
        }
    }
}
