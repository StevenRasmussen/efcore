// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query
{
    /// <summary>
    ///     <para>
    ///         A class that translate queryable methods in a query.
    ///     </para>
    ///     <para>
    ///         This type is typically used by database providers (and other extensions). It is generally
    ///         not used in application code.
    ///     </para>
    /// </summary>
    public abstract class QueryableMethodTranslatingExpressionVisitor : ExpressionVisitor
    {
        private readonly bool _subquery;
        private readonly EntityShaperNullableMarkingExpressionVisitor _entityShaperNullableMarkingExpressionVisitor;

        /// <summary>
        ///     Creates a new instance of the <see cref="QueryableMethodTranslatingExpressionVisitor" /> class.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this class. </param>
        /// <param name="queryCompilationContext"> The query compilation context object to use. </param>
        /// <param name="subquery"> A bool value indicating whether it is for a subquery translation. </param>
        protected QueryableMethodTranslatingExpressionVisitor(
            [NotNull] QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
            [NotNull] QueryCompilationContext queryCompilationContext,
            bool subquery)
        {
            Check.NotNull(dependencies, nameof(dependencies));
            Check.NotNull(queryCompilationContext, nameof(queryCompilationContext));

            Dependencies = dependencies;
            QueryCompilationContext = queryCompilationContext;
            _subquery = subquery;
            _entityShaperNullableMarkingExpressionVisitor = new EntityShaperNullableMarkingExpressionVisitor();
        }

        /// <summary>
        ///     Parameter object containing service dependencies.
        /// </summary>
        protected virtual QueryableMethodTranslatingExpressionVisitorDependencies Dependencies { get; }

        /// <summary>
        ///     The query compilation context object for current compilation.
        /// </summary>
        protected virtual QueryCompilationContext QueryCompilationContext { get; }

        /// <inheritdoc />
        protected override Expression VisitExtension(Expression extensionExpression)
        {
            Check.NotNull(extensionExpression, nameof(extensionExpression));

            return extensionExpression switch
            {
                ShapedQueryExpression _ => extensionExpression,
                QueryRootExpression queryRootExpression => CreateShapedQueryExpression(queryRootExpression.EntityType),
                _ => base.VisitExtension(extensionExpression),
            };
        }

        /// <inheritdoc />
        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            Check.NotNull(methodCallExpression, nameof(methodCallExpression));

            ShapedQueryExpression CheckTranslated(ShapedQueryExpression translated)
            {
                return translated ?? throw new InvalidOperationException(CoreStrings.TranslationFailed(methodCallExpression.Print()));
            }

            var method = methodCallExpression.Method;
            if (method.DeclaringType == typeof(Queryable)
                || method.DeclaringType == typeof(QueryableExtensions))
            {
                var source = Visit(methodCallExpression.Arguments[0]);
                if (source is ShapedQueryExpression shapedQueryExpression)
                {
                    var genericMethod = method.IsGenericMethod ? method.GetGenericMethodDefinition() : null;
                    switch (method.Name)
                    {
                        case nameof(Queryable.All)
                            when genericMethod == QueryableMethods.All:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateAll(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.Any)
                            when genericMethod == QueryableMethods.AnyWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateAny(shapedQueryExpression, null));

                        case nameof(Queryable.Any)
                            when genericMethod == QueryableMethods.AnyWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateAny(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.AsQueryable)
                            when genericMethod == QueryableMethods.AsQueryable:
                            return source;

                        case nameof(Queryable.Average)
                            when QueryableMethods.IsAverageWithoutSelector(method):
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateAverage(shapedQueryExpression, null, methodCallExpression.Type));

                        case nameof(Queryable.Average)
                            when QueryableMethods.IsAverageWithSelector(method):
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateAverage(shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type));

                        case nameof(Queryable.Cast)
                            when genericMethod == QueryableMethods.Cast:
                            return CheckTranslated(TranslateCast(shapedQueryExpression, method.GetGenericArguments()[0]));

                        case nameof(Queryable.Concat)
                            when genericMethod == QueryableMethods.Concat:
                        {
                            var source2 = Visit(methodCallExpression.Arguments[1]);
                            if (source2 is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return TranslateConcat(
                                    shapedQueryExpression,
                                    innerShapedQueryExpression);
                            }

                            break;
                        }

                        case nameof(Queryable.Contains)
                            when genericMethod == QueryableMethods.Contains:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateContains(shapedQueryExpression, methodCallExpression.Arguments[1]));

                        case nameof(Queryable.Count)
                            when genericMethod == QueryableMethods.CountWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateCount(shapedQueryExpression, null));

                        case nameof(Queryable.Count)
                            when genericMethod == QueryableMethods.CountWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateCount(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.DefaultIfEmpty)
                            when genericMethod == QueryableMethods.DefaultIfEmptyWithoutArgument:
                            return CheckTranslated(TranslateDefaultIfEmpty(shapedQueryExpression, null));

                        case nameof(Queryable.DefaultIfEmpty)
                            when genericMethod == QueryableMethods.DefaultIfEmptyWithArgument:
                            return CheckTranslated(TranslateDefaultIfEmpty(shapedQueryExpression, methodCallExpression.Arguments[1]));

                        case nameof(Queryable.Distinct)
                            when genericMethod == QueryableMethods.Distinct:
                            return CheckTranslated(TranslateDistinct(shapedQueryExpression));

                        case nameof(Queryable.ElementAt)
                            when genericMethod == QueryableMethods.ElementAt:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateElementAtOrDefault(shapedQueryExpression, methodCallExpression.Arguments[1], false));

                        case nameof(Queryable.ElementAtOrDefault)
                            when genericMethod == QueryableMethods.ElementAtOrDefault:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(
                                TranslateElementAtOrDefault(shapedQueryExpression, methodCallExpression.Arguments[1], true));

                        case nameof(Queryable.Except)
                            when genericMethod == QueryableMethods.Except:
                        {
                            var source2 = Visit(methodCallExpression.Arguments[1]);
                            if (source2 is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return CheckTranslated(
                                    TranslateExcept(
                                        shapedQueryExpression,
                                        innerShapedQueryExpression));
                            }

                            break;
                        }

                        case nameof(Queryable.First)
                            when genericMethod == QueryableMethods.FirstWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateFirstOrDefault(shapedQueryExpression, null, methodCallExpression.Type, false));

                        case nameof(Queryable.First)
                            when genericMethod == QueryableMethods.FirstWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateFirstOrDefault(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type, false));

                        case nameof(Queryable.FirstOrDefault)
                            when genericMethod == QueryableMethods.FirstOrDefaultWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(TranslateFirstOrDefault(shapedQueryExpression, null, methodCallExpression.Type, true));

                        case nameof(Queryable.FirstOrDefault)
                            when genericMethod == QueryableMethods.FirstOrDefaultWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(
                                TranslateFirstOrDefault(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type, true));

                        case nameof(Queryable.GroupBy)
                            when genericMethod == QueryableMethods.GroupByWithKeySelector:
                            return CheckTranslated(TranslateGroupBy(shapedQueryExpression, GetLambdaExpressionFromArgument(1), null, null));

                        case nameof(Queryable.GroupBy)
                            when genericMethod == QueryableMethods.GroupByWithKeyElementSelector:
                            return CheckTranslated(
                                TranslateGroupBy(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), GetLambdaExpressionFromArgument(2), null));

                        case nameof(Queryable.GroupBy)
                            when genericMethod == QueryableMethods.GroupByWithKeyElementResultSelector:
                            return CheckTranslated(
                                TranslateGroupBy(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), GetLambdaExpressionFromArgument(2),
                                    GetLambdaExpressionFromArgument(3)));

                        case nameof(Queryable.GroupBy)
                            when genericMethod == QueryableMethods.GroupByWithKeyResultSelector:
                            return CheckTranslated(
                                TranslateGroupBy(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), null, GetLambdaExpressionFromArgument(2)));

                        case nameof(Queryable.GroupJoin)
                            when genericMethod == QueryableMethods.GroupJoin:
                        {
                            if (Visit(methodCallExpression.Arguments[1]) is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return CheckTranslated(
                                    TranslateGroupJoin(
                                        shapedQueryExpression,
                                        innerShapedQueryExpression,
                                        GetLambdaExpressionFromArgument(2),
                                        GetLambdaExpressionFromArgument(3),
                                        GetLambdaExpressionFromArgument(4)));
                            }

                            break;
                        }

                        case nameof(Queryable.Intersect)
                            when genericMethod == QueryableMethods.Intersect:
                        {
                            if (Visit(methodCallExpression.Arguments[1]) is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return CheckTranslated(TranslateIntersect(shapedQueryExpression, innerShapedQueryExpression));
                            }

                            break;
                        }

                        case nameof(Queryable.Join)
                            when genericMethod == QueryableMethods.Join:
                        {
                            if (Visit(methodCallExpression.Arguments[1]) is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return CheckTranslated(
                                    TranslateJoin(
                                        shapedQueryExpression, innerShapedQueryExpression, GetLambdaExpressionFromArgument(2),
                                        GetLambdaExpressionFromArgument(3), GetLambdaExpressionFromArgument(4)));
                            }

                            break;
                        }

                        case nameof(QueryableExtensions.LeftJoin)
                            when genericMethod == QueryableExtensions.LeftJoinMethodInfo:
                        {
                            if (Visit(methodCallExpression.Arguments[1]) is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return CheckTranslated(
                                    TranslateLeftJoin(
                                        shapedQueryExpression, innerShapedQueryExpression, GetLambdaExpressionFromArgument(2),
                                        GetLambdaExpressionFromArgument(3), GetLambdaExpressionFromArgument(4)));
                            }

                            break;
                        }

                        case nameof(Queryable.Last)
                            when genericMethod == QueryableMethods.LastWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateLastOrDefault(shapedQueryExpression, null, methodCallExpression.Type, false));

                        case nameof(Queryable.Last)
                            when genericMethod == QueryableMethods.LastWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateLastOrDefault(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type, false));

                        case nameof(Queryable.LastOrDefault)
                            when genericMethod == QueryableMethods.LastOrDefaultWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(TranslateLastOrDefault(shapedQueryExpression, null, methodCallExpression.Type, true));

                        case nameof(Queryable.LastOrDefault)
                            when genericMethod == QueryableMethods.LastOrDefaultWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(
                                TranslateLastOrDefault(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type, true));

                        case nameof(Queryable.LongCount)
                            when genericMethod == QueryableMethods.LongCountWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateLongCount(shapedQueryExpression, null));

                        case nameof(Queryable.LongCount)
                            when genericMethod == QueryableMethods.LongCountWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateLongCount(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.Max)
                            when genericMethod == QueryableMethods.MaxWithoutSelector:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateMax(shapedQueryExpression, null, methodCallExpression.Type));

                        case nameof(Queryable.Max)
                            when genericMethod == QueryableMethods.MaxWithSelector:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateMax(shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type));

                        case nameof(Queryable.Min)
                            when genericMethod == QueryableMethods.MinWithoutSelector:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateMin(shapedQueryExpression, null, methodCallExpression.Type));

                        case nameof(Queryable.Min)
                            when genericMethod == QueryableMethods.MinWithSelector:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateMin(shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type));

                        case nameof(Queryable.OfType)
                            when genericMethod == QueryableMethods.OfType:
                            return CheckTranslated(TranslateOfType(shapedQueryExpression, method.GetGenericArguments()[0]));

                        case nameof(Queryable.OrderBy)
                            when genericMethod == QueryableMethods.OrderBy:
                            return CheckTranslated(TranslateOrderBy(shapedQueryExpression, GetLambdaExpressionFromArgument(1), true));

                        case nameof(Queryable.OrderByDescending)
                            when genericMethod == QueryableMethods.OrderByDescending:
                            return CheckTranslated(TranslateOrderBy(shapedQueryExpression, GetLambdaExpressionFromArgument(1), false));

                        case nameof(Queryable.Reverse)
                            when genericMethod == QueryableMethods.Reverse:
                            return CheckTranslated(TranslateReverse(shapedQueryExpression));

                        case nameof(Queryable.Select)
                            when genericMethod == QueryableMethods.Select:
                            return CheckTranslated(TranslateSelect(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.SelectMany)
                            when genericMethod == QueryableMethods.SelectManyWithoutCollectionSelector:
                            return CheckTranslated(TranslateSelectMany(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.SelectMany)
                            when genericMethod == QueryableMethods.SelectManyWithCollectionSelector:
                            return CheckTranslated(
                                TranslateSelectMany(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), GetLambdaExpressionFromArgument(2)));

                        case nameof(Queryable.Single)
                            when genericMethod == QueryableMethods.SingleWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateSingleOrDefault(shapedQueryExpression, null, methodCallExpression.Type, false));

                        case nameof(Queryable.Single)
                            when genericMethod == QueryableMethods.SingleWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateSingleOrDefault(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type, false));

                        case nameof(Queryable.SingleOrDefault)
                            when genericMethod == QueryableMethods.SingleOrDefaultWithoutPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(TranslateSingleOrDefault(shapedQueryExpression, null, methodCallExpression.Type, true));

                        case nameof(Queryable.SingleOrDefault)
                            when genericMethod == QueryableMethods.SingleOrDefaultWithPredicate:
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.SingleOrDefault);
                            return CheckTranslated(
                                TranslateSingleOrDefault(
                                    shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type, true));

                        case nameof(Queryable.Skip)
                            when genericMethod == QueryableMethods.Skip:
                            return CheckTranslated(TranslateSkip(shapedQueryExpression, methodCallExpression.Arguments[1]));

                        case nameof(Queryable.SkipWhile)
                            when genericMethod == QueryableMethods.SkipWhile:
                            return CheckTranslated(TranslateSkipWhile(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.Sum)
                            when QueryableMethods.IsSumWithoutSelector(method):
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(TranslateSum(shapedQueryExpression, null, methodCallExpression.Type));

                        case nameof(Queryable.Sum)
                            when QueryableMethods.IsSumWithSelector(method):
                            shapedQueryExpression = shapedQueryExpression.UpdateResultCardinality(ResultCardinality.Single);
                            return CheckTranslated(
                                TranslateSum(shapedQueryExpression, GetLambdaExpressionFromArgument(1), methodCallExpression.Type));

                        case nameof(Queryable.Take)
                            when genericMethod == QueryableMethods.Take:
                            return CheckTranslated(TranslateTake(shapedQueryExpression, methodCallExpression.Arguments[1]));

                        case nameof(Queryable.TakeWhile)
                            when genericMethod == QueryableMethods.TakeWhile:
                            return CheckTranslated(TranslateTakeWhile(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                        case nameof(Queryable.ThenBy)
                            when genericMethod == QueryableMethods.ThenBy:
                            return CheckTranslated(TranslateThenBy(shapedQueryExpression, GetLambdaExpressionFromArgument(1), true));

                        case nameof(Queryable.ThenByDescending)
                            when genericMethod == QueryableMethods.ThenByDescending:
                            return CheckTranslated(TranslateThenBy(shapedQueryExpression, GetLambdaExpressionFromArgument(1), false));

                        case nameof(Queryable.Union)
                            when genericMethod == QueryableMethods.Union:
                        {
                            if (Visit(methodCallExpression.Arguments[1]) is ShapedQueryExpression innerShapedQueryExpression)
                            {
                                return CheckTranslated(TranslateUnion(shapedQueryExpression, innerShapedQueryExpression));
                            }

                            break;
                        }

                        case nameof(Queryable.Where)
                            when genericMethod == QueryableMethods.Where:
                            return CheckTranslated(TranslateWhere(shapedQueryExpression, GetLambdaExpressionFromArgument(1)));

                            LambdaExpression GetLambdaExpressionFromArgument(int argumentIndex) =>
                                methodCallExpression.Arguments[argumentIndex].UnwrapLambdaFromQuote();
                    }
                }
            }

            return _subquery
                ? (Expression)null
                : throw new NotImplementedException(CoreStrings.UnhandledMethod(method.Name));
        }

        private sealed class EntityShaperNullableMarkingExpressionVisitor : ExpressionVisitor
        {
            protected override Expression VisitExtension(Expression extensionExpression)
            {
                Check.NotNull(extensionExpression, nameof(extensionExpression));

                return extensionExpression is EntityShaperExpression entityShaper
                    ? entityShaper.MarkAsNullable()
                    : base.VisitExtension(extensionExpression);
            }
        }

        /// <summary>
        ///     Marks entity shaper in given shaper expression as nullable.
        /// </summary>
        /// <param name="shaperExpression"> The shaper expression to process. </param>
        /// <returns> New shaper expression in which all entity shapers are nullable. </returns>
        protected virtual Expression MarkShaperNullable([NotNull] Expression shaperExpression)
        {
            Check.NotNull(shaperExpression, nameof(shaperExpression));

            return _entityShaperNullableMarkingExpressionVisitor.Visit(shaperExpression);
        }

        /// <summary>
        ///      Translates result selector for join operation.
        /// </summary>
        /// <param name="outer"> The shaped query expression for outer source. The join on the query expression is already performed on outer query expression. </param>
        /// <param name="resultSelector"> The result selector lambda to translate. </param>
        /// <param name="innerShaper"> The shaper for inner source. </param>
        /// <param name="transparentIdentifierType"> The clr type of transparent identifier created from result. </param>
        /// <returns> The shaped query expression after translation of result selector. </returns>
        protected virtual ShapedQueryExpression TranslateResultSelectorForJoin(
            [NotNull] ShapedQueryExpression outer,
            [NotNull] LambdaExpression resultSelector,
            [NotNull] Expression innerShaper,
            [NotNull] Type transparentIdentifierType)
        {
            Check.NotNull(outer, nameof(outer));
            Check.NotNull(resultSelector, nameof(resultSelector));
            Check.NotNull(innerShaper, nameof(innerShaper));
            Check.NotNull(transparentIdentifierType, nameof(transparentIdentifierType));

            outer = outer.UpdateShaperExpression(
                CombineShapers(outer.QueryExpression, outer.ShaperExpression, innerShaper, transparentIdentifierType));

            var transparentIdentifierParameter = Expression.Parameter(transparentIdentifierType);

            Expression original1 = resultSelector.Parameters[0];
            var replacement1 = AccessOuterTransparentField(transparentIdentifierType, transparentIdentifierParameter);
            Expression original2 = resultSelector.Parameters[1];
            var replacement2 = AccessInnerTransparentField(transparentIdentifierType, transparentIdentifierParameter);
            var newResultSelector = Expression.Lambda(
                new ReplacingExpressionVisitor(
                    new[] { original1, original2 }, new[] { replacement1, replacement2 })
                    .Visit(resultSelector.Body),
                transparentIdentifierParameter);

            return TranslateSelect(outer, newResultSelector);
        }

        private Expression CombineShapers(
            Expression queryExpression,
            Expression outerShaper,
            Expression innerShaper,
            Type transparentIdentifierType)
        {
            var outerMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Outer");
            var innerMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Inner");
            outerShaper = new MemberAccessShiftingExpressionVisitor(queryExpression, outerMemberInfo).Visit(outerShaper);
            innerShaper = new MemberAccessShiftingExpressionVisitor(queryExpression, innerMemberInfo).Visit(innerShaper);

            return Expression.New(
                transparentIdentifierType.GetTypeInfo().DeclaredConstructors.Single(),
                new[] { outerShaper, innerShaper }, outerMemberInfo, innerMemberInfo);
        }

        private sealed class MemberAccessShiftingExpressionVisitor : ExpressionVisitor
        {
            private readonly Expression _queryExpression;
            private readonly MemberInfo _memberShift;

            public MemberAccessShiftingExpressionVisitor(Expression queryExpression, MemberInfo memberShift)
            {
                _queryExpression = queryExpression;
                _memberShift = memberShift;
            }

            protected override Expression VisitExtension(Expression node)
            {
                Check.NotNull(node, nameof(node));

                return node is ProjectionBindingExpression projectionBindingExpression
                    ? new ProjectionBindingExpression(
                        _queryExpression,
                        projectionBindingExpression.ProjectionMember.Prepend(_memberShift),
                        projectionBindingExpression.Type)
                    : base.VisitExtension(node);
            }
        }

        private static Expression AccessOuterTransparentField(
            Type transparentIdentifierType,
            Expression targetExpression)
        {
            var fieldInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Outer");

            return Expression.Field(targetExpression, fieldInfo);
        }

        private static Expression AccessInnerTransparentField(
            Type transparentIdentifierType,
            Expression targetExpression)
        {
            var fieldInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Inner");

            return Expression.Field(targetExpression, fieldInfo);
        }

        /// <summary>
        ///     Translates given subquery by creating a subquery visitor.
        /// </summary>
        /// <param name="expression"> The subquery expression to translate. </param>
        /// <returns> The translation of given subquery. </returns>
        public virtual ShapedQueryExpression TranslateSubquery([NotNull] Expression expression)
        {
            Check.NotNull(expression, nameof(expression));

            return (ShapedQueryExpression)CreateSubqueryVisitor().Visit(expression);
        }

        /// <summary>
        ///     Creates a visitor customized to translate subquery.
        /// </summary>
        /// <returns> A visitor to translate subquery. </returns>
        protected abstract QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor();

        /// <summary>
        ///     Creates a <see cref="ShapedQueryExpression"/> for given type by finding it's entity type in the model.
        /// </summary>
        /// <param name="elementType"> The clr type of the entity type to look for. </param>
        /// <returns> A shaped query expression for given clr type. </returns>
        [Obsolete("Use overload which takes IEntityType.")]
        protected abstract ShapedQueryExpression CreateShapedQueryExpression([NotNull] Type elementType);
        /// <summary>
        ///     Creates a <see cref="ShapedQueryExpression"/> for given entity type.
        /// </summary>
        /// <param name="entityType"> The the entity type. </param>
        /// <returns> A shaped query expression for given entity type. </returns>
        protected abstract ShapedQueryExpression CreateShapedQueryExpression([NotNull] IEntityType entityType);

        /// <summary>
        ///     Translates Queryable.All method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateAll([NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression predicate);

        /// <summary>
        ///     Translates Queryable.Any method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateAny([NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression predicate);

        /// <summary>
        ///     Translates Queryable.Average method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="selector"> The selector used with method. </param>
        /// <param name="resultType"> The result type after the operation. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateAverage(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression selector, [NotNull] Type resultType);

        /// <summary>
        ///     Translates Queryable.Cast method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="castType"> The type result is being casted to. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateCast([NotNull] ShapedQueryExpression source, [NotNull] Type castType);

        /// <summary>
        ///     Translates Queryable.Concat method over given source.
        /// </summary>
        /// <param name="source1"> The shaped query on which the operator is applied. </param>
        /// <param name="source2"> The other source to perform concat. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateConcat(
            [NotNull] ShapedQueryExpression source1, [NotNull] ShapedQueryExpression source2);

        /// <summary>
        ///     Translates Queryable.Contains method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="item"> The item to search for. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateContains([NotNull] ShapedQueryExpression source, [NotNull] Expression item);

        /// <summary>
        ///     Translates Queryable.Count method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateCount(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression predicate);

        /// <summary>
        ///     Translates Queryable.DefaultIfEmpty method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="defaultValue"> The default value to use. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateDefaultIfEmpty(
            [NotNull] ShapedQueryExpression source, [CanBeNull] Expression defaultValue);

        /// <summary>
        ///     Translates Queryable.Distinct method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateDistinct([NotNull] ShapedQueryExpression source);

        /// <summary>
        ///     Translates Queryable.ElementAt/ElementAtOrDefault method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="index"> The index of the element. </param>
        /// <param name="returnDefault"> A value indicating whether default should be returned or throw. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateElementAtOrDefault(
            [NotNull] ShapedQueryExpression source, [NotNull] Expression index, bool returnDefault);

        /// <summary>
        ///     Translates Queryable.Except method over given source.
        /// </summary>
        /// <param name="source1"> The shaped query on which the operator is applied. </param>
        /// <param name="source2"> The other source to perform except with. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateExcept(
            [NotNull] ShapedQueryExpression source1, [NotNull] ShapedQueryExpression source2);

        /// <summary>
        ///     Translates Queryable.First/FirstOrDefault method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <param name="returnType"> The return type of result. </param>
        /// <param name="returnDefault"> A value indicating whether default should be returned or throw. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateFirstOrDefault(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression predicate, [NotNull] Type returnType, bool returnDefault);

        /// <summary>
        ///     Translates Queryable.GroupBy method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="keySelector"> The key selector used with method. </param>
        /// <param name="elementSelector"> The element selector used with method. </param>
        /// <param name="resultSelector"> The result selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateGroupBy(
            [NotNull] ShapedQueryExpression source,
            [NotNull] LambdaExpression keySelector,
            [CanBeNull] LambdaExpression elementSelector,
            [CanBeNull] LambdaExpression resultSelector);

        /// <summary>
        ///     Translates Queryable.GroupJoin method over given source.
        /// </summary>
        /// <param name="outer"> The shaped query on which the operator is applied. </param>
        /// <param name="inner"> The inner shaped query to perform join with. </param>
        /// <param name="outerKeySelector"> The key selector for outer source. </param>
        /// <param name="innerKeySelector"> The key selector for inner source. </param>
        /// <param name="resultSelector"> The result selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateGroupJoin(
            [NotNull] ShapedQueryExpression outer,
            [NotNull] ShapedQueryExpression inner,
            [NotNull] LambdaExpression outerKeySelector,
            [NotNull] LambdaExpression innerKeySelector,
            [NotNull] LambdaExpression resultSelector);

        /// <summary>
        ///     Translates Queryable.Intersect method over given source.
        /// </summary>
        /// <param name="source1"> The shaped query on which the operator is applied. </param>
        /// <param name="source2"> The other source to perform intersect with. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateIntersect(
            [NotNull] ShapedQueryExpression source1, [NotNull] ShapedQueryExpression source2);

        /// <summary>
        ///     Translates Queryable.Join method over given source.
        /// </summary>
        /// <param name="outer"> The shaped query on which the operator is applied. </param>
        /// <param name="inner"> The inner shaped query to perform join with. </param>
        /// <param name="outerKeySelector"> The key selector for outer source. </param>
        /// <param name="innerKeySelector"> The key selector for inner source. </param>
        /// <param name="resultSelector"> The result selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateJoin(
            [NotNull] ShapedQueryExpression outer,
            [NotNull] ShapedQueryExpression inner,
            [CanBeNull] LambdaExpression outerKeySelector,
            [CanBeNull] LambdaExpression innerKeySelector,
            [NotNull] LambdaExpression resultSelector);

        /// <summary>
        ///     Translates LeftJoin method over given source.
        /// </summary>
        /// <param name="outer"> The shaped query on which the operator is applied. </param>
        /// <param name="inner"> The inner shaped query to perform join with. </param>
        /// <param name="outerKeySelector"> The key selector for outer source. </param>
        /// <param name="innerKeySelector"> The key selector for inner source. </param>
        /// <param name="resultSelector"> The result selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateLeftJoin(
            [NotNull] ShapedQueryExpression outer,
            [NotNull] ShapedQueryExpression inner,
            [CanBeNull] LambdaExpression outerKeySelector,
            [CanBeNull] LambdaExpression innerKeySelector,
            [NotNull] LambdaExpression resultSelector);

        /// <summary>
        ///     Translates Queryable.Last/LastOrDefault method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <param name="returnType"> The return type of result. </param>
        /// <param name="returnDefault"> A value indicating whether default should be returned or throw. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateLastOrDefault(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression predicate, [NotNull] Type returnType, bool returnDefault);

        /// <summary>
        ///     Translates Queryable.LongCount method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateLongCount(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression predicate);

        /// <summary>
        ///     Translates Queryable.Max method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="selector"> The selector used with method. </param>
        /// <param name="resultType"> The result type after the operation. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateMax(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression selector, [NotNull] Type resultType);

        /// <summary>
        ///     Translates Queryable.Min method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="selector"> The selector used with method. </param>
        /// <param name="resultType"> The result type after the operation. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateMin(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression selector, [NotNull] Type resultType);

        /// <summary>
        ///     Translates Queryable.OfType method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="resultType"> The type of result which is being filtered with. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateOfType([NotNull] ShapedQueryExpression source, [NotNull] Type resultType);

        /// <summary>
        ///     Translates Queryable.OrderBy/OrderByDescending method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="keySelector"> The key selector used with method. </param>
        /// <param name="ascending"> A value indicating whether the ordering is ascending or not. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateOrderBy(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression keySelector, bool ascending);

        /// <summary>
        ///     Translates Queryable.Reverse method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateReverse([NotNull] ShapedQueryExpression source);

        /// <summary>
        ///     Translates Queryable.Select method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="selector"> The selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSelect(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression selector);

        /// <summary>
        ///     Translates Queryable.SelectMany method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="collectionSelector"> The collection selector used with method. </param>
        /// <param name="resultSelector"> The result selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSelectMany(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression collectionSelector, [NotNull] LambdaExpression resultSelector);

        /// <summary>
        ///     Translates Queryable.SelectMany method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="selector"> The selector used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSelectMany(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression selector);

        /// <summary>
        ///     Translates Queryable.Single/SingleOrDefault method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <param name="returnType"> The return type of result. </param>
        /// <param name="returnDefault"> A value indicating whether default should be returned or throw. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSingleOrDefault(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression predicate, [NotNull] Type returnType, bool returnDefault);

        /// <summary>
        ///     Translates Queryable.Skip method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="count"> The count used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSkip(
            [NotNull] ShapedQueryExpression source, [NotNull] Expression count);

        /// <summary>
        ///     Translates Queryable.SkipWhile method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSkipWhile(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression predicate);

        /// <summary>
        ///     Translates Queryable.Sum method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="selector"> The selector used with method. </param>
        /// <param name="resultType"> The result type after the operation. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateSum(
            [NotNull] ShapedQueryExpression source, [CanBeNull] LambdaExpression selector, [NotNull] Type resultType);

        /// <summary>
        ///     Translates Queryable.Take method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="count"> The count used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateTake([NotNull] ShapedQueryExpression source, [NotNull] Expression count);

        /// <summary>
        ///     Translates Queryable.TakeWhile method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateTakeWhile(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression predicate);

        /// <summary>
        ///     Translates Queryable.ThenBy/ThenByDescending method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="keySelector"> The key selector used with method. </param>
        /// <param name="ascending"> A value indicating whether the ordering is ascending or not. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateThenBy(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression keySelector, bool ascending);

        /// <summary>
        ///     Translates Queryable.Union method over given source.
        /// </summary>
        /// <param name="source1"> The shaped query on which the operator is applied. </param>
        /// <param name="source2"> The other source to perform union with. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateUnion(
            [NotNull] ShapedQueryExpression source1, [NotNull] ShapedQueryExpression source2);

        /// <summary>
        ///     Translates Queryable.Where method over given source.
        /// </summary>
        /// <param name="source"> The shaped query on which the operator is applied. </param>
        /// <param name="predicate"> The predicate used with method. </param>
        /// <returns> The shaped query after translation. </returns>
        protected abstract ShapedQueryExpression TranslateWhere(
            [NotNull] ShapedQueryExpression source, [NotNull] LambdaExpression predicate);
    }
}
