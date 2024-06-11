//! This module defines the actual node of an Acyclic Join.
//!
//! It defines the struct [`AcyclicJoinNode`] which implements [`UserDefinedLogicalNodeCore`]

use core::fmt;
use std::{collections::HashMap, sync::Arc};

use crate::hyper::{
    hypergraph::{HyperGraph, VertexIndex},
    join_tree,
};
use datafusion::{
    common::{Column, DFSchema, DFSchemaRef, Result},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, Expr, Extension, LogicalPlan, Operator, UserDefinedLogicalNodeCore,
    },
    physical_plan::internal_err,
};

use crate::equivalence_list::EquiClassList;
// join trees in this context are acyclic hypergraphs
pub type JoinTree = join_tree::JoinTree<Vec<Column>, Arc<LogicalPlan>>;
pub type JoinTreeNode = join_tree::JoinTreeNode<Vec<Column>, Arc<LogicalPlan>>;

/// An Acyclic Join (acyclic conjunctive query) that can be placed in a [`LogicalPlan`]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AcyclicJoin {
    /// the resulting join tree from the inputs
    pub join_tree: JoinTree,

    // original inputs of the Join tree this was constructed out of
    relations: Vec<Arc<LogicalPlan>>,

    // all the equi-join predicates of every join node
    predicates: Vec<(Expr, Expr)>,

    // the computed output schema from the join_tree
    schema: DFSchemaRef,
}

impl AcyclicJoin {
    /// Try's to create a AcyclicJoin from a collection of inputs and equi-join predicates,
    /// where:
    /// - the inputs are the inputs of a join tree
    /// - the predicates are all the equi-join predicates of that join tree
    ///
    ///
    /// If everything goes well and the INNER JOIN is acyclic this function will return `Ok(Some(Self))`  
    /// if the INNER JOIN is cyclic, this will return `Ok(None)`
    /// if the inputs are malformed, this will return Err([`DataFusionError`])
    ///
    ///
    /// NOTE: This function can best be used within Optimizer Rules
    // FIXME: predicates can only contain fields that are in the inputs, check this how?
    pub fn try_new(
        inputs: Vec<Arc<LogicalPlan>>,
        predicates: Vec<(Expr, Expr)>,
    ) -> Result<Option<Self>> {
        // TODO: If these are ever merged into DataFusion codebase, transform these to internals
        if inputs.len() < 2 {
            return Err(DataFusionError::External(
                format!(
                    "AcyclicJoin::try_new expects atleast 2 input relations, got: {}",
                    inputs.len()
                )
                .into(),
            ));
        }
        if predicates.is_empty() {
            return Err(DataFusionError::External(
                "AcyclicJoin::try_new expects atleast 1 join predicate".into(),
            ));
        }
        let mut hg = HyperGraph::with_capacity(inputs.len(), predicates.len());

        // we need to resolve the names of the equi-join predicates
        // let's say you have: `A.a = B.b`, then `B.b` has to point to the same vertex as `A.a`
        // then you have `A.a = C.c`, then `A.a`, `B.b` and `C.c` all have to point to the same vertex
        // how?
        // with an equivalence map
        let resolved_predicates: EquiClassList<_> = predicates
            .iter()
            .filter_map(|(left, right)| {
                Some((left.try_into_col().ok()?, right.try_into_col().ok()?))
            })
            .collect();

        for set in resolved_predicates.iter() {
            hg.add_vertex(set.iter().cloned().collect());
        }
        // we don't add the vertices that are not in the join predicates
        // TODO: still do this? or change HyperGraph
        for input in inputs.iter() {
            let vertices = input
                .schema()
                .fields()
                .iter()
                // Safety: we just created the indices from resolved_predicates
                .filter_map(|field| unsafe {
                    Some(VertexIndex::new(
                        resolved_predicates.get_class_idx(&field.qualified_column())?,
                    ))
                })
                .collect();
            hg.add_hyper_edge(vertices, Arc::clone(input))
                .map_err(|e| DataFusionError::External(e.into()))?;
        }

        let join_tree =
            hg.into_join_tree_with(|edge, _vertices| JoinTreeNode::new(Arc::clone(&edge.data)));

        Ok(join_tree.map(|join_tree| Self {
            predicates,
            schema: Self::compute_jointree_schema(&join_tree),
            join_tree,
            relations: inputs,
        }))
    }

    /// Computes the ouput schema of an acyclic join tree
    /// It computes it by iterating over the tree left-right, top-down and pushing the fields
    /// of the schema to the output schema.
    ///
    /// ## Visual example
    /// Note: columns with the same name are the join columns for simplicity of this example
    /// ```text
    ///               R1(a, b, c)
    ///                /        \
    ///         R2(a, b, d)   R3(c, e, f)
    ///                           \
    ///                         R4(e, g, h)
    /// ```
    /// the output_schema of this join tree will be:
    /// ```text
    /// [r1.a, r1.b, r1.c, r2.a, r2.b, r2.d, r3.c, r3.e, r3.f, r4.e, r4.g, r4.h]
    /// ```
    ///
    /// You can use it like so:
    /// ## Example
    /// ```ignore
    /// let hg = HyperGraph::new();
    /// let join_tree = hg.into_join_tree();
    /// let output_schema = AcyclicJoinNode::compute_jointree_schema(&join_tree);
    /// // use schema...
    /// ```
    ///
    pub fn compute_jointree_schema(tree: &JoinTree) -> DFSchemaRef {
        let Some(node) = tree.get_root() else {
            return Arc::new(DFSchema::empty());
        };
        // Add all fields of the root's guard to the output schema
        let mut output_schema = Vec::new();

        let mut to_visit = Vec::with_capacity(16); // 16 join relations, seems appropiate?
        to_visit.push(node);

        // Traverse through jointree (left-to-right, top-down).
        while let Some(node) = to_visit.pop() {
            output_schema.extend(node.data.schema().fields().clone());
            to_visit.extend(node.children.iter().rev().map(|cn| &cn.child));
        }

        Arc::new(DFSchema::new_with_metadata(output_schema, HashMap::new()).unwrap())
    }
}

impl UserDefinedLogicalNodeCore for AcyclicJoin {
    fn name(&self) -> &str {
        "AcyclicJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.relations.iter().map(AsRef::as_ref).collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    /// Returns the equi-join predicates in the same way [`LogicalPlan::Join`] does.
    fn expressions(&self) -> Vec<Expr> {
        self.predicates
            .iter()
            .map(|(left, right)| Expr::eq(left.clone(), right.clone()))
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AcyclicJoin")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert!(
            inputs.len() > 1,
            "Acyclicjoin can only be made by a minimum of 2 inputs"
        );
        assert!(
            !exprs.is_empty(),
            "AcyclicJoin must atleast have 1 predicate"
        );

        // from_template gives us a guarantee that self.from_template(exprs, _).expressions() == exprs
        // so we know the form of the given expressions are the equi-join predicates
        let new_predicates = exprs
            .iter()
            .map(|equi_expr| {
                // We do this the same way LogicalPlan::Join does it
                let unalias_expr = equi_expr.clone().unalias();
                if let Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }) = unalias_expr
                {
                    Ok((*left, *right))
                } else {
                    internal_err!(
                        "AcyclicJoin only accepts binary equality expressions, actual:{equi_expr}"
                    )
                }
            })
            .collect::<Result<Vec<(Expr, Expr)>>>()
            // `from_template` doesn't return Results (which is weird) so we unwrap that internal error
            .unwrap();

        let new_inputs = inputs.iter().cloned().map(Arc::new).collect();
        Self::try_new(new_inputs, new_predicates)
            .expect("There should be no errors constructing from a template")
            .expect("Acyclicjoins should be created from templates")
    }
}

impl From<AcyclicJoin> for LogicalPlan {
    fn from(value: AcyclicJoin) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{table_scan, Expr, LogicalPlan};

    use datafusion::common::{Column, Result};
    use itertools::Itertools;

    use super::AcyclicJoin;

    macro_rules! column {
        ($table_name:expr, $column_name:expr) => {
            Expr::Column(Column::new(Some($table_name), $column_name))
        };
    }

    #[test]
    fn no_predicate() -> Result<()> {
        let t1 = Arc::new(two_attr_table("t1")?);
        let t2 = Arc::new(two_attr_table("t2")?);

        // no predicates -> no acyclic join?
        assert!(AcyclicJoin::try_new(vec![t1, t2], vec![]).is_err());

        Ok(())
    }

    #[test]
    fn binary_join() -> Result<()> {
        let t1 = Arc::new(two_attr_table("t1")?);
        let t2 = Arc::new(two_attr_table("t2")?);
        let predicates = vec![(column!("t1", "id"), column!("t2", "foreign_id"))];

        // binary join = acyclic join
        assert!(AcyclicJoin::try_new(vec![t1, t2], predicates)?.is_some());

        Ok(())
    }

    /// T(a, _), R(b, _), S(a, b, _)
    #[test]
    fn broken_cycle_join() -> Result<()> {
        let t1 = Arc::new(two_attr_table("t1")?);
        let t2 = Arc::new(two_attr_table("t2")?);
        let t3 = Arc::new(three_attr_table("t3")?);
        let predicates = vec![
            (column!("t1", "id"), column!("t3", "foreign_id1")),
            (column!("t2", "id"), column!("t3", "foreign_id2")),
        ];

        // binary join = acyclic join
        let node = AcyclicJoin::try_new(vec![t1, t2, t3], predicates)?;
        assert!(dbg!(node).is_some());

        Ok(())
    }

    fn two_attr_table(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("foreign_id", DataType::UInt32, false),
        ]);

        table_scan(Some(name), &schema, None)?.build()
    }

    fn three_attr_table(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("foreign_id1", DataType::UInt32, false),
            Field::new("foreign_id2", DataType::UInt32, false),
        ]);

        table_scan(Some(name), &schema, None)?.build()
    }

    fn table_with_attrs(name: &str, attrs: &[&str]) -> Result<LogicalPlan> {
        let schema = Schema::new(
            attrs
                .iter()
                .map(|&name| Field::new(name, DataType::Int32, false))
                .collect_vec(),
        );
        table_scan(Some(name), &schema, None)?.build()
    }
}
