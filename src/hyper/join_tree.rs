//! This module contains the code fro creating and using a join Tree (HyperTree)
use std::fmt::{self, Display};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ChildNode<V, HE> {
    pub child: JoinTreeNode<V, HE>,
    pub joined_on: Vec<V>,
}

/// A node in a Join Tree containing it's children, data and it's name
/// A join tree is essentialy a acyclic undirected hypergraph
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinTreeNode<V, HE> {
    pub data: HE,
    pub children: Vec<ChildNode<V, HE>>,
}

impl<V, HE> JoinTreeNode<V, HE> {
    /// Construct a new Node with a name
    ///
    /// # Example
    /// ```
    /// use hyper::join_tree::*;
    ///
    /// let node = JoinTreeNode::<&str, _>::new("node");
    /// assert_eq!(node.data, "node");
    /// ```
    pub fn new(data: HE) -> Self {
        Self {
            data,
            children: vec![],
        }
    }

    /// Add a new child to this node and returns a reference to that child node
    /// Children are always contained in order in which they were added
    /// # Examples
    /// ```
    /// use hyper::join_tree::*;
    ///
    /// let mut parent = JoinTreeNode::new(vec!["rel_atom_1", "rel_atom_2"]);
    /// assert_eq!(parent.data, &["rel_atom_1", "rel_atom_2"]);
    /// let child = parent.add_child(JoinTreeNode::new(vec!["rel_atom_3", "rel_atom_1"]), vec!["rel_atom_1"]);
    /// assert_eq!(child.data, vec!["rel_atom_3", "rel_atom_1"]);
    /// assert!(!child.data.is_empty());
    /// ```
    /// Because it is a reference borrowing rules still apply
    /// ```compile_fail
    /// let mut parent = JoinTreeNode::new("vec!["rel_atom_1", "rel_atom_2"]);
    /// let child = parent.add_child(JoinTreeNode::new("vec!["rel_atom_3", "rel_atom_2"]), vec!["rel_atom_2"]);
    /// let child_2 = parent.add_child(JoinTreeNode::new("vec!["rel_atom_3", "rel_atom_2"]),vec!["rel_atom_2"]);
    /// println!("{}", child.data);
    /// ```
    pub fn add_child(
        &mut self,
        child: JoinTreeNode<V, HE>,
        joined_on: Vec<V>,
    ) -> &JoinTreeNode<V, HE> {
        self.children.push(ChildNode { joined_on, child });
        &self.children.last().unwrap().child
    }

    /// Add multiple children to this node and returns a slice to those that were added
    /// Children are always contained in order in which they were added
    /// # Examples
    /// ```compile_fail
    /// use hyper::join_tree::*;
    ///
    /// let mut parent = JoinTreeNode::new(vec!["rel_atom_1", "rel_atom_2"]);
    /// assert_eq!(parent.data, &["rel_atom_1", "rel_atom_2"]);
    /// let children = parent.add_children(vec![JoinTreeNode::new(vec![])]);
    /// assert!(children[0].data.is_empty());
    /// ```
    /// Because it is a reference borrowing rules still apply
    /// ```compile_fail
    /// use hyper::join_tree::*;
    ///
    /// let mut parent = JoinTreeNode::new(vec!["rel_atom_1", "rel_atom_2"]);
    /// assert_eq!(parent.name, "parent");
    /// assert_eq!(parent.data, &["rel_atom_1", "rel_atom_2"]);
    ///
    /// let children = parent.add_children(vec![JoinTreeNode::new("child_1")]);
    ///
    /// assert_eq!(children[0].name, "child");
    /// assert!(children[0].data.is_empty());
    /// let child = parent.add_child(JoinTreeNode::new("child_2"));
    /// // borrow invalid after mut borrow of parent above
    /// println!("{:?}", children);
    /// ```
    pub fn add_children(&mut self, children: Vec<JoinTreeNode<V, HE>>) -> &[JoinTreeNode<V, HE>] {
        unimplemented!()
        // let len = self.children.len();
        // self.children.extend(children);
        // &self.children.as_slice()[len..]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinTree<V, HE> {
    root: Option<JoinTreeNode<V, HE>>,
}

impl<V, HE> JoinTree<V, HE> {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn with_root(root: JoinTreeNode<V, HE>) -> Self {
        JoinTree { root: Some(root) }
    }

    pub fn get_root(&self) -> Option<&JoinTreeNode<V, HE>> {
        self.root.as_ref()
    }

    pub fn set_root(&mut self, root: JoinTreeNode<V, HE>) {
        self.root = Some(root);
    }

    pub fn display_graphviz(&self) -> impl Display
    where
        V: fmt::Debug + Clone,
        HE: fmt::Debug + Clone,
    {
        let root = match self.root {
            Some(ref root) => {
                let mut to_visit = vec![(
                    1,
                    0,
                    ChildNode {
                        child: root.clone(),
                        joined_on: vec![],
                    },
                )];
                let mut new_n = 2;
                let mut result = String::new();

                while let Some((parent, this_n, node)) = to_visit.pop() {
                    let graph_node = format!(
                        "{this_n}[shape=box label=\"{}\"]",
                        format!("{:?}", node.child.data).replace('\"', "")
                    );
                    let graph_arrow = format!(
                        "{parent} -> {this_n} [arrowhead=normal, arrowtail=none, dir=forward, labeldistance=\"0.0\", label=\"{}\"]",
                        format!("{:?}", node.joined_on).replace('\"', "")
                    );
                    result.push_str(&format!("{}\n\t\t{}\n\t\t", graph_node, graph_arrow));

                    for child in &node.child.children {
                        new_n += 1;
                        to_visit.push((this_n, new_n, Clone::clone(child)));
                    }
                }
                result
            }
            None => String::new(),
        };
        let tree = format!(
            "digraph{{\n\tsubgraph cluster_1{{\n\t\tgraph[label=\"join_tree\"]\n\t\t1[shape=box label=\"root\"]\n\t\t{}\n\t}}\n}}",
            root
        );

        tree
    }
}

impl<V, HE> Default for JoinTree<V, HE> {
    fn default() -> Self {
        Self::new()
    }
}
