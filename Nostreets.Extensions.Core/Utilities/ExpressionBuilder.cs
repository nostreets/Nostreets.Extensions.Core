using Nostreets.Extensions.Extend.Basic;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Nostreets.Extensions.Utilities
{

    public static class ExpressionHelper
    {
        public static Func<T, R> Func<T, R>(Func<T, R> f) => f;

        public static Expression<Func<T, R>> Expr<T, R>(Expression<Func<T, R>> f) => f;

        public static Expression<Func<T, R>> Expr<T, R>(Expression func)
        {
            return Expression.Lambda<Func<T, R>>(func);
        }

        public static Expression Expr(Expression func, Type t, Type r)
        {
            Type castedFunc = typeof(Func<,>).IntoGenericConstructorAsT(new[] { t, r });
            object result = typeof(Expression).IntoGenericMethod("Lambda", castedFunc, func);
            return (Expression)result;
        }


    }


    public static class ExpressionBuilder
    {
        private static MethodInfo containsMethod = typeof(string).GetMethod("Contains");
        private static MethodInfo startsWithMethod = typeof(string).GetMethod("StartsWith", new Type[] { typeof(string) });
        private static MethodInfo endsWithMethod = typeof(string).GetMethod("EndsWith", new Type[] { typeof(string) });


        public static Expression<Func<T, bool>> GetPredicate<T>(IList<PartialExp> filters)
        {
            if (filters.Count == 0)
                return null;

            ParameterExpression param = Expression.Parameter(typeof(T), "t");
            Expression exp = null;

            if (filters.Count == 1)
                exp = GetExpression(param, filters[0]);
            else if (filters.Count == 2)
                exp = GetANDExpression(param, filters[0], filters[1]);
            else
            {
                while (filters.Count > 0)
                {
                    var f1 = filters[0];
                    var f2 = filters[1];

                    if (exp == null)
                        exp = GetANDExpression(param, filters[0], filters[1]);
                    else
                        exp = Expression.AndAlso(exp, GetANDExpression(param, filters[0], filters[1]));

                    filters.Remove(f1);
                    filters.Remove(f2);

                    if (filters.Count == 1)
                    {
                        exp = Expression.AndAlso(exp, GetExpression(param, filters[0]));
                        filters.RemoveAt(0);
                    }
                }
            }

            return Expression.Lambda<Func<T, bool>>(exp, param);
        }

        //private static Expression GetExpression<T>(ParameterExpression param, Filter filter)
        //{
        //    MemberExpression member = Expression.Property(param, filter.PropertyName);
        //    ConstantExpression constant = Expression.Constant(filter.Value);

        //    switch (filter.Operation)
        //    {
        //        case Op.Equals:
        //            return Expression.Equal(member, constant);

        //        case Op.GreaterThan:
        //            return Expression.GreaterThan(member, constant);

        //        case Op.GreaterThanOrEqual:
        //            return Expression.GreaterThanOrEqual(member, constant);

        //        case Op.LessThan:
        //            return Expression.LessThan(member, constant);

        //        case Op.LessThanOrEqual:
        //            return Expression.LessThanOrEqual(member, constant);

        //        case Op.Contains:
        //            return Expression.Call(member, containsMethod, constant);

        //        case Op.StartsWith:
        //            return Expression.Call(member, startsWithMethod, constant);

        //        case Op.EndsWith:
        //            return Expression.Call(member, endsWithMethod, constant);
        //    }

        //    return null;
        //}

        //private static BinaryExpression GetExpression<T>(ParameterExpression param, Filter filter1, Filter filter2)
        //{
        //    Expression bin1 = GetExpression<T>(param, filter1);
        //    Expression bin2 = GetExpression<T>(param, filter2);

        //    return Expression.AndAlso(bin1, bin2);
        //}

        public static Expression<Func<T, bool>> GetPredicate<T>(IList<PartialExp> filters, Type type)
        {
            if (filters.Count == 0)
                return null;

            ParameterExpression param = Expression.Parameter(typeof(T), "t");
            Expression exp = null;

            if (filters.Count == 1)
                exp = GetExpression(param, filters[0]);
            else if (filters.Count == 2)
                exp = GetANDExpression(param, filters[0], filters[1]);
            else
            {
                while (filters.Count > 0)
                {
                    var f1 = filters[0];
                    var f2 = filters[1];

                    if (exp == null)
                        exp = GetANDExpression(param, filters[0], filters[1]);
                    else
                        exp = Expression.AndAlso(exp, GetANDExpression(param, filters[0], filters[1]));

                    filters.Remove(f1);
                    filters.Remove(f2);

                    if (filters.Count == 1)
                    {
                        exp = Expression.AndAlso(exp, GetExpression(param, filters[0]));
                        filters.RemoveAt(0);
                    }
                }
            }

            return Expression.Lambda<Func<T, bool>>(exp, param);
        }

        private static Expression GetExpression(ParameterExpression param, PartialExp filter)
        {
            MemberExpression member = Expression.Property(param, filter.PropertyName);
            ConstantExpression constant = Expression.Constant(filter.Value);

            switch (filter.Operation)
            {
                case Op.Equals:
                    return Expression.Equal(member, constant);

                case Op.GreaterThan:
                    return Expression.GreaterThan(member, constant);

                case Op.GreaterThanOrEqual:
                    return Expression.GreaterThanOrEqual(member, constant);

                case Op.LessThan:
                    return Expression.LessThan(member, constant);

                case Op.LessThanOrEqual:
                    return Expression.LessThanOrEqual(member, constant);

                case Op.Contains:
                    return Expression.Call(member, containsMethod, constant);

                case Op.StartsWith:
                    return Expression.Call(member, startsWithMethod, constant);

                case Op.EndsWith:
                    return Expression.Call(member, endsWithMethod, constant);
            }

            return null;
        }

        private static BinaryExpression GetANDExpression(ParameterExpression param, PartialExp filter1, PartialExp filter2)
        {
            Expression bin1 = GetExpression(param, filter1);
            Expression bin2 = GetExpression(param, filter2);

            return Expression.AndAlso(bin1, bin2);
        }
    }

    public class PartialExp
    {
        public PartialExp() { }
        public PartialExp(string propertyName, Op vs, object value)
        {
            PropertyName = propertyName;
            Value = value;
            Operation = vs;
        }

        public string PropertyName { get; set; }
        public Op Operation { get; set; }
        public object Value { get; set; }
    }

    public enum Op
    {
        Equals,
        GreaterThan,
        LessThan,
        GreaterThanOrEqual,
        LessThanOrEqual,
        Contains,
        StartsWith,
        EndsWith

    }
}
