//! This module defines the optimizer that tries to detect Joins as Acyclic Joins.
//!
//! It defines [`AcyclicJoinOptimizerRule`] which implements [`OptimizerRule `].
//! If the optimizer is succesfull it will return a new [`LogicalPlan`] with the Ayclic Joins
//! replaced by a [`AcyclicJoinNode`]

use std::sync::Arc;

use datafusion::{
    common::Result,
    logical_expr::{Expr, Join, JoinType, LogicalPlan},
    optimizer::{optimize_children, OptimizerConfig, OptimizerRule},
};

use crate::node::AcyclicJoin;

/// Will try to detect Acyclic INNER JOINs in a Logical Plan and convert the INNER JOIN tree
/// to an [`AcyclicJoinNode`].
pub struct AcyclicJoinOptimizerRule {}

impl AcyclicJoinOptimizerRule {}

// FIXME: Filters of the joins are not handled properly
impl OptimizerRule for AcyclicJoinOptimizerRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(
                join @ Join {
                    join_type: JoinType::Inner,
                    filter: None,
                    on,
                    ..
                },
                // we need atleast 1 equi-join predicate
            ) if !on.is_empty() => {
                let spj = JoinContext::from_join(join);

                if spj.inputs.len() <= 2 {
                    return optimize_children(self, plan, config);
                }

                AcyclicJoin::try_new(spj.inputs, spj.predicates)?
                    .map(Into::into)
                    .map_or_else(
                        || optimize_children(self, plan, config),
                        |p| Ok(Some(optimize_children(self, &p, config)?.unwrap_or(p))),
                    )
            }
            _ => optimize_children(self, plan, config),
        }
    }

    fn name(&self) -> &str {
        "AcyclicJoin Optimizer"
    }
}

/// Holds information of a [`Join`]
pub struct JoinContext {
    /// The inputs in to the join
    pub inputs: Vec<Arc<LogicalPlan>>,

    /// The equi-predicates of the join
    pub predicates: Vec<(Expr, Expr)>,
}

impl JoinContext {
    /// Transforms a [`Join`] in to [`Self`](`JoinContext`)
    pub fn from_join(join: &Join) -> Self {
        Self::collect_join_context(join)
    }

    /// Take a node of a Join Tree and find every input to this (sub)-tree and for every join node found
    /// collect it's equi-join predicates
    ///
    /// Returns the inputs and all the predicates
    ///
    /// # Visual example
    /// ```text
    ///                  inner join: a.x = b.x
    ///                     /                 \
    ///           inner join: a.y = c.y     tablescan: b(x, t)
    ///             /              \
    ///   tablescan a(x, y)     tablescan c(y, z)
    /// ```
    ///
    /// if we pass this tree to this function it wil return:
    /// - inputs: [ [`TableScan`]:a , [`TableScan`]: c , [`TableScan`]: b ]
    /// - predicates: [ `(a.x, b.x)`, `(a.y, c.y)`]
    ///
    /// # Example
    /// this is an example directly from [`AcyclicJoinOptimizerRule::try_optimize`] with some details left out
    /// ```
    /// # use datafusion::optimizer::OptimizerRule;
    /// # use datafusion::logical_expr::{JoinType, Join,LogicalPlan};
    /// # use datafusion::optimizer::OptimizerConfig;
    /// # use datafusion::common::*;
    /// # use acyclicjoin_node::optimizer::AcyclicJoinOptimizerRule;
    /// // our own optimizer
    /// struct Optimizer{};
    /// impl OptimizerRule for Optimizer{
    ///    fn try_optimize(
    ///        &self,
    ///        plan: &LogicalPlan,
    ///        config: &dyn OptimizerConfig,
    ///    ) -> Result<Option<LogicalPlan>> {
    ///        if let LogicalPlan::Join(
    ///            join @ Join {
    ///                join_type: JoinType::Inner,
    ///                ..
    ///            },
    ///        ) = plan
    ///        {
    ///            let j_ctx = JoinContext::collect_join_context(join);
    ///            // do something with the inputs and predicates of this join-tree
    ///            # return Ok(Some(plan.clone()));
    ///        }
    ///        // ..
    ///        # Ok(None)
    ///    }
    ///
    ///    // some other impls
    /// # fn name(&self) -> &str {""}
    /// }    
    /// ```
    ///
    /// # Panics
    /// Panics if the given join is not an INNER JOIN
    pub fn collect_join_context(join: &Join) -> JoinContext {
        assert_eq!(join.join_type, JoinType::Inner);

        let mut predicates: Vec<_> = join.on.clone();
        let mut inputs = Vec::with_capacity(8);
        let mut to_visit = Vec::with_capacity(32); // 32 pointers, seems appropiate
        to_visit.push(&join.left);
        to_visit.push(&join.right);

        while let Some(node) = to_visit.pop() {
            // if we get a projection check it's input, because we can get INNERJOIN -> projection -> INNERJOIN
            let to_test = if let LogicalPlan::Projection(plan) = node.as_ref() {
                &plan.input
            } else {
                node
            };
            match to_test.as_ref() {
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on,
                    filter: None,
                    join_type: JoinType::Inner,
                    ..
                }) => {
                    to_visit.push(left);
                    to_visit.push(right);

                    for pred in on.iter() {
                        if !predicates.contains(pred) {
                            predicates.push(pred.clone());
                        }
                    }
                }

                // anything else is a input
                _ => inputs.push(node.clone()),
            }
        }

        Self { inputs, predicates }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::Column,
        logical_expr::{table_scan, JoinType, LogicalPlan, LogicalPlanBuilder},
        optimizer::{optimizer::Optimizer, OptimizerContext, OptimizerRule},
    };

    use datafusion::common::Result;

    use super::AcyclicJoinOptimizerRule;

    fn _observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}
    fn assert_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        let opt = AcyclicJoinOptimizerRule {};
        let opt_context = OptimizerContext::new().with_max_passes(1);

        let optimizer = Optimizer::with_rules(vec![Arc::new(opt)]);
        let optimized_plan = optimizer.optimize(&plan, &opt_context, _observe)?;
        let displayed_plan = format!("{optimized_plan:?}");

        assert_eq!(displayed_plan, expected);
        Ok(())
    }

    #[test]
    fn no_predicates() -> Result<()> {
        let t1 = simple_table("t1")?;
        let t2 = simple_table("t2")?;
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                None,
            )?
            .build()?;

        assert_plan_equal(plan.clone(), &format!("{plan:?}"))?;
        Ok(())
    }
    #[ignore = "I don't know how to resolve binary joins"]
    #[test]
    fn binary_join_one_predicate() -> Result<()> {
        let t1 = simple_table("t1")?;
        let t2 = simple_table("t2")?;
        let plan = build_join(t1, t2, "t1.foreigd_id", "t2.id")?;

        let expected = "AcyclicJoin\
            \n  TableScan: t2\
            \n  TableScan: t1";

        assert_plan_equal(plan, expected)?;
        Ok(())
    }

    #[test]
    fn ternary_join() -> Result<()> {
        let t1 = simple_table("t1")?;
        let t2 = simple_table("t2")?;
        let t3 = extra_table("t3")?;
        let binary_join = build_join(t1, t3, "t1.id", "t3.foreign_id1")?;
        let final_join = build_join(binary_join, t2, "t2.id", "t3.foreign_id2")?;

        let expected = "AcyclicJoin\
            \n  TableScan: t2\
            \n  TableScan: t3\
            \n  TableScan: t1";
        assert_plan_equal(final_join, expected)?;
        Ok(())
    }

    #[ignore = "Cyclic join has a binary join, don't know how to handle it yet."] // it finds a binary join, so wait for it
    #[test]
    fn cyclic_join() -> Result<()> {
        let t1 = simple_table("t1")?;
        // to create cycle, we create another tablescan from the same table
        let second_t1 = simple_table("t1")?;
        let t2 = simple_table("t2")?;
        let t3 = simple_table("t3")?;
        let first_join = build_join(t1, t2, "t1.foreign_id", "t2.id")?;
        let second_join = build_join(first_join.clone(), t3, "t2.foreign_id", "t3.id")?;
        let cyclic_join = build_join(second_join, second_t1, "t3.foreign_id", "t1.id")?;

        let expected = format!("{cyclic_join:?}");
        assert_plan_equal(cyclic_join, &expected)?;
        Ok(())
    }

    fn build_join(
        left_table: LogicalPlan,
        right_table: LogicalPlan,
        left_key: &str,
        right_key: &str,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left_table)
            .join(
                right_table,
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name(left_key)],
                    vec![Column::from_qualified_name(right_key)],
                ),
                None,
            )?
            .build()
    }

    fn simple_table(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("foreign_id", DataType::UInt32, false),
        ]);
        let t = table_scan(Some(name), &schema, None)?.build()?;
        Ok(t)
    }

    fn extra_table(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("foreign_id1", DataType::UInt32, false),
            Field::new("foreign_id2", DataType::UInt32, false),
        ]);
        let t3 = table_scan(Some(name), &schema, None)?.build()?;
        Ok(t3)
    }
}
