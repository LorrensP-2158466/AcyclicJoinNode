use std::fmt::Display;
use std::sync::Arc;

use async_recursion::async_recursion;
use async_trait::async_trait;

use datafusion::common::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};

use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use yannakakais::multisemijoin::MultiSemiJoin;
use yannakakais::YannakakisFullJoinExec;
use yannakakis_join_implementation::yannakakis as yannakakais;
use yannakakis_join_implementation::yannakakis::groupby::GroupBy;

use crate::node::{AcyclicJoin, JoinTreeNode};

pub struct AcyclicJoinQueryPlanner {}

#[async_trait]
impl QueryPlanner for AcyclicJoinQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Credits to arrow-datafusion/datafusion/core/tests/user_defined/user_defined_plan.rs

        // Teach the default physical planner how to plan AcyclicJoins.
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            AcyclicJoinExecPlanner {},
        )]);
        // Delegate most work of physical planning to the default physical planner
        Ok(physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
            .unwrap())
    }
}

// fn msj_into_graphviz_helper(msj: &MultiSemiJoin, root: usize, this: usize) -> String {
//     let mut to_visit = vec![(1, 0, msj)];
//     let mut new_n = 2;
//     let mut result = String::new();

//     while let Some((parent, this_n, node)) = to_visit.pop() {
//         let graph_node = format!(
//             "{this_n}[shape=box label=\"{}\"]",
//             format!("{:?}", node.data).replace('\"', "")
//         );
//         let graph_arrow = format!(
//                         "{parent} -> {this_n} [arrowhead=normal, arrowtail=none, dir=forward, labeldistance=\"0.0\", label=\"{}\"]",
//                         format!("{:?}", node.joined_on).replace('\"', "")
//                     );
//         result.push_str(&format!("{}\n\t\t{}\n\t\t", graph_node, graph_arrow));

//         for child in &node.child.children {
//             new_n += 1;
//             to_visit.push((this_n, new_n, Clone::clone(child)));
//         }
//     }
//     let tree = format!(
//             "digraph{{\n\tsubgraph cluster_1{{\n\t\tgraph[label=\"join_tree\"]\n\t\t1[shape=box label=\"root\"]\n\t\t{}\n\t}}\n}}",
//             root
//         );

//     tree
// }

/// Helper that transforms MultiSemiJoin into GraphViz tree
// TODO: create some graphviz for MultiSemiJoin
#[allow(unused)]
fn msj_into_graphviz(msj: &MultiSemiJoin) -> impl Display {
    // Boilerplate structure to wrap LogicalPlan with something
    // that that can be formatted
    ""
}

pub struct AcyclicJoinExecPlanner {}

impl AcyclicJoinExecPlanner {
    /// Creates a [`MultiSemiJoin`] from an [`AcyclicJoinTreeNode`].
    ///
    /// Note: we require at least one node so we don't have to check null roots
    #[async_recursion]
    pub async fn create_physical_join_tree(
        node: &JoinTreeNode,
        planner: &dyn PhysicalPlanner,
        state: &SessionState,
    ) -> Result<MultiSemiJoin> {
        // i got the physical inputs, but i have no way of finding out which is the root
        // HACK: recreating the physicalplan with the planner and state
        // TODO: find a way
        let guard = planner
            .create_physical_plan(&node.data, state)
            .await
            .unwrap();
        let guard_fields = node.data.schema().fields();
        // hold the list of grouped children and list of equi join keys
        let mut grouped_children = Vec::new();
        let mut all_equi_join_keys = Vec::new();

        // recursively transform the children into grouped multisemijoins
        // this will go left-right top-down
        for child_node in node.children.iter() {
            let child_fields = child_node.child.data.schema().fields();

            // From YannakakisFullJoinExec we know that the group columns are the indices into
            // the relation's fields that are joined on
            let group_columns: Vec<_> = child_fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    // so we check if this field is join on
                    child_node
                        .joined_on
                        .iter()
                        .any(|v| v.contains(&f.qualified_column()))
                })
                .map(|(i, _)| i)
                .collect();

            // create equi-join-keys from this node and its child
            // (guard_index, group_index) is the form of a equi-join-key pair
            // so for every guard column we check that if it is joined on a group_column index
            // than we collect the index of that group_column and the guard
            let mut equi_join_keys = Vec::new();
            for (guard_idx, guard_column) in guard_fields.iter().enumerate() {
                let guard_column = &guard_column.qualified_column();
                for (group_idx, column_idx) in group_columns.iter().enumerate() {
                    let child_column = &child_fields[*column_idx].qualified_column();

                    if child_node
                        .joined_on
                        .iter()
                        .any(|v| v.contains(child_column) && v.contains(guard_column))
                    {
                        equi_join_keys.push((guard_idx, group_idx));
                    };
                }
            }
            all_equi_join_keys.push(equi_join_keys);

            // the recursive call to create a semijoin of the child to place in the groupby
            grouped_children.push(Arc::new(GroupBy::new(
                Self::create_physical_join_tree(&child_node.child, planner, state)
                    .await
                    .unwrap(),
                group_columns,
            )));
        }

        Ok(MultiSemiJoin::new(
            guard,
            grouped_children,
            all_equi_join_keys,
        ))
    }
}

#[async_trait]
impl ExtensionPlanner for AcyclicJoinExecPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(acj_node) = node.as_any().downcast_ref::<AcyclicJoin>() {
            // transform Join Tree to MultiSemiJoin
            let join_tree = &acj_node.join_tree;
            let Some(root) = join_tree.get_root() else {
                return Ok(None);
            };
            let root_multi_semi_join =
                Self::create_physical_join_tree(root, planner, session_state)
                    .await
                    .unwrap();

            // construct the yannakakis plan
            let yannakakis_join =
                Arc::new(YannakakisFullJoinExec::new(root_multi_semi_join).unwrap());
            return Ok(Some(yannakakis_join));
        }

        Ok(None)
    }
}
