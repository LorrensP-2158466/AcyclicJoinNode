//! This module contains algortihms and datastructures for the implementation of a Hypergraph.  
//! In particular for use within [Conjuctive Queries](https://en.wikipedia.org/wiki/Conjunctive_query) and determining if it is acyclic,
//! acyclic QQ's are usefull in query optimizations and can be more efficiently evaluated than normal CQ's
//!
//! If a hypergraph is acyclic it can be turned into a JoinTree([Read more..](JoinTree))
use super::join_tree::{JoinTree, JoinTreeNode};
use ahash::AHasher;
use slab::Slab;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::fmt::{self, Display};
use std::hash::BuildHasherDefault;

//TODO: Use some form of arena so when calling `into_join_tree` or `into_join_tree_with` can run faster

/// A Hyperedge used in [`HyperGraph`]  
/// It stores the vertices that are contained in this hyperedge, it's name (debugging relations) and it's id
#[derive(Debug, Clone)]
pub struct HyperEdge<T> {
    vertices: HashSet<VertexIndex>,
    /// Name of the relation or Hyperedge
    pub data: T,
}

impl<T> HyperEdge<T>
where
    T: Clone + fmt::Debug,
{
    pub fn contains_vertex(&self, v: &VertexIndex) -> bool {
        self.vertices.contains(v)
    }
}

/// Index used to find a HyperEdge in a HyperGraph
/// Using indices from hypergraphs in other hypergraphs is considered unsound
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HyperEdgeIndex(usize);

/// A Vertex used in [`HyperGraph`]  
/// It stores the edges it is contained in and it's name (debugging relations)
#[derive(Debug, Clone)]
pub struct Vertex<T> {
    edges: HashSet<HyperEdgeIndex>,
    pub data: T,
}

/// Index used to find a Vertex in a HyperGraph
/// Using indices from hypergraphs in other hypergraphs is considered unsound
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VertexIndex(usize);

impl VertexIndex {
    /// # Safety
    /// The caller must guarentee that `idx` is a valid index into a [`HyperGraph`]
    pub unsafe fn new(idx: usize) -> Self {
        VertexIndex(idx)
    }
}

/// An ear of an hypergraph H is an edge E of an other edge F both in H where:
/// - every edge of E is only in E
/// - every edge of E is in E and also in F (F will be the parent)
/// this struct holds the ear edge and it's parent, it can happen that a parent is not present
/// it is up to the user to determine it's parent (if needed)
#[derive(Debug, Clone)]
pub struct Ear<'hg, HG: Clone + fmt::Debug> {
    ear: &'hg HyperEdge<HG>,
    parent: Option<&'hg HyperEdge<HG>>,
}

impl<'hg, HG> Ear<'hg, HG>
where
    HG: fmt::Debug + Clone,
{
    pub fn new(ear: &'hg HyperEdge<HG>, parent: Option<&'hg HyperEdge<HG>>) -> Self {
        Self { ear, parent }
    }
}

/// This is the main implementation of a [hypergraph](https://en.wikipedia.org/wiki/Hypergraph)
/// a Hypergraph is a Graph structure but where edges (hyperedges) can join multiple vertices
///
/// there are lot of algorithms and other things that can be done with this datastructure but
/// this implementation is only used for Hypergraphs of [Conjuctive Queries](https://en.wikipedia.org/wiki/Conjunctive_query)
/// and to check wether that CQ is Acyclic and thus a [JoinTree] can be constructed
#[derive(Clone, Default)]
pub struct HyperGraph<V, HG> {
    edges: Slab<HyperEdge<HG>>,
    vertices: Slab<Vertex<V>>,
}

impl<V, HG> fmt::Debug for HyperGraph<V, HG>
where
    V: fmt::Debug,
    HG: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperGraph{V, E}:")
            .field("V", &self.vertices)
            .field("E", &self.edges)
            .finish()
    }
}

impl<V, HG> fmt::Display for HyperGraph<V, HG>
where
    V: fmt::Debug,
    HG: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "HyperGraph{{V, E}}:")?;
        writeln!(f, "\tV: {:#?}", self.vertices.iter().collect::<Vec<_>>())?;
        writeln!(f, "\tH: {:#?}", self.edges.iter().collect::<Vec<_>>())
    }
}
impl<V, HE> HyperGraph<V, HE>
where
    V: Clone + fmt::Debug,
    HE: Clone + fmt::Debug,
{
    /// Construct a new Hypergraph
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let hg = HyperGraph::<(),()>::new();
    /// //...
    ///
    /// ```
    pub fn new() -> Self {
        Self {
            edges: Slab::new(),
            vertices: Slab::new(),
        }
    }

    /// Construct a new empty Hypergraph with specified capacity for both the edges and vertices.
    /// Because the underlying structure depends on [`Vec`], capacity can be larger than dan the specified amount.  
    /// See [`Vec::with_capacity`](Vec::with_capacity) for more info on how this works.
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let hg = HyperGraph::<(),()>::with_capacity(10, 5);
    /// ```
    pub fn with_capacity(edges_capacity: usize, vertices_capacity: usize) -> Self {
        Self {
            edges: Slab::with_capacity(edges_capacity),
            vertices: Slab::with_capacity(vertices_capacity),
        }
    }

    /// Returns the total number of edges this Hypergraph can hold without reallocating
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::<(),()>::with_capacity(5, 10);
    /// assert_eq!(hg.edges_count(), 0);
    /// assert!(hg.edges_capacity() >= 5);
    /// ```
    pub fn edges_capacity(&self) -> usize {
        self.edges.capacity()
    }

    /// Returns the total number of vertices this Hypergraph can hold without reallocating
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::<(),()>::with_capacity(5, 10);
    /// assert_eq!(hg.vertices_count(), 0);
    /// assert!(hg.vertices_capacity() >= 10);
    /// ```
    pub fn vertices_capacity(&self) -> usize {
        self.vertices.capacity()
    }

    /// Returns amount of hyperedges in the hypergraph
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::<(), _>::new();
    /// assert_eq!(hg.edges_count(), 0);
    /// assert!(hg.add_hyper_edge([].into(), "1").is_ok());
    /// assert_eq!(hg.edges_count(), 1);
    /// ```
    pub fn edges_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns amount of vertices in the hypergraph
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::<_,()>::new();
    /// assert_eq!(hg.vertices_count(), 0);
    /// hg.add_vertex("v1");
    /// assert_eq!(hg.vertices_count(), 1);
    /// ```
    pub fn vertices_count(&self) -> usize {
        self.vertices.len()
    }

    /// Checks if the hypergraph has no hyperedges and no vertices
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// assert!(hg.is_empty());
    /// let v1 = hg.add_vertex("v1");
    /// assert!(!hg.is_empty());
    /// hg.remove_vertex(v1);
    ///
    /// let e1 = hg.add_hyper_edge([].into(), "e1").unwrap(); // we can unwrap because hyperedge is empty
    /// assert!(!hg.is_empty());
    /// hg.remove_hyper_edge(e1);
    /// assert!(hg.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty() && self.vertices.is_empty()
    }

    /// Adds a vertex to the hyperedge and returns it's index  
    /// See [`VertexIndex`] for more details
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let v2 = hg.add_vertex("v2");
    /// hg.add_hyper_edge([v1, v2].into(), "he1");
    /// assert_ne!(v1, v2); // indices are unique
    /// ```
    pub fn add_vertex(&mut self, data: V) -> VertexIndex {
        VertexIndex(self.vertices.insert(Vertex {
            edges: HashSet::new(),
            data,
        }))
    }

    /// Removes a vertex of the hyperedge and returns it if it existed  
    /// All other hyperedges that contained this vertex lose their reference to this vertex
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::<_, ()>::new();
    /// let v1 = hg.add_vertex("v1");
    /// let deleted_v1 = hg.remove_vertex(v1).unwrap(); // we just saw it happen :)
    /// assert_eq!(deleted_v1.data, "v1");
    /// ```
    pub fn remove_vertex(&mut self, index: VertexIndex) -> Option<Vertex<V>> {
        for (_, edge) in self.edges.iter_mut() {
            edge.vertices.retain(|&i| i != index)
        }
        self.vertices.try_remove(index.0)
    }

    /// Adds a hyperedge to the hypergraph and if all of it's vertices are contained within the graph
    /// it will return the index otherwise an error
    ///
    ///
    /// The expected vertex indices container is a [`HashSet`] but constant arrays `[T; N]` can be turned into
    /// Hashsets. so the following is recommended
    /// # Examples
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let v2 = hg.add_vertex("v2");
    /// let he1 = hg.add_hyper_edge([v1, v2].into(), "he1");
    /// ```
    ///
    /// HashSet also implements `FromIterator<T>` so collecting from iterators is also possible:
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let v2 = hg.add_vertex("v2");
    /// // say you have a vec of those indices
    /// let indices = vec![v1, v2];
    /// let he1 = hg.add_hyper_edge(indices.into_iter().collect(), "he1"); // into_iterator because we need the indices, not the references
    /// ```
    pub fn add_hyper_edge(
        &mut self,
        vertices: HashSet<VertexIndex>,
        data: HE,
    ) -> Result<HyperEdgeIndex, &'static str> {
        if !vertices.iter().all(|i| self.vertices.get(i.0).is_some()) {
            return Err("Only vertices that are known");
        }

        let key = HyperEdgeIndex(self.edges.vacant_key());
        for vertex_i in &vertices {
            self.vertices.get_mut(vertex_i.0).unwrap().edges.insert(key);
        }
        // slab guarentees that idx == key
        let idx = HyperEdgeIndex(self.edges.insert(HyperEdge { vertices, data }));
        Ok(idx)
    }

    /// If the hyperedge exists, the method will remove it and return it, otherwise it will return `None`  
    /// Every vertex that has a reference to this edge will lose that reference
    ///
    /// You can still check if the edge contains vertices but that is the only public behaviour for now...
    /// all other vertices in the graph do lose reference to this edge
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let v2 = hg.add_vertex("v2");
    /// let he1 = hg.add_hyper_edge([v1, v2].into(), "he1").unwrap();
    /// let he1 = hg.remove_hyper_edge(he1).unwrap();
    /// // as you can see, you can check it
    /// assert!(he1.contains_vertex(&v1));
    /// assert!(he1.contains_vertex(&v2));
    /// ```
    pub fn remove_hyper_edge(&mut self, edge: HyperEdgeIndex) -> Option<HyperEdge<HE>> {
        for (_, vertex) in self.vertices.iter_mut() {
            vertex.edges.retain(|&i| i != edge)
        }
        self.edges.try_remove(edge.0)
    }

    /// Returns a reference to the indices of hyperedges the vertex is contained in
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let v2 = hg.add_vertex("v2");
    /// let he1 = hg.add_hyper_edge([v1, v2].into(), "he1").unwrap();
    /// let he2 = hg.add_hyper_edge([v1].into(), "he2").unwrap();
    /// assert_eq!(hg.get_vertex_edges(&v1), &[he1, he2].into_iter().collect());
    /// assert_eq!(hg.get_vertex_edges(&v2), &[he1].into_iter().collect());
    /// ```
    pub fn get_vertex_edges(&self, index: &VertexIndex) -> &HashSet<HyperEdgeIndex> {
        &self.vertices.get(index.0).unwrap().edges
    }

    /// Returns a reference of a vertex corresponding with the given index, if it exists
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let he1 = hg.add_hyper_edge([v1].into(), "he1").unwrap();
    /// assert!(hg.get_vertex(&v1).is_some());
    /// assert_eq!(hg.get_vertex(&v1).unwrap().data, "v1");
    /// ```
    pub fn get_vertex(&self, index: &VertexIndex) -> Option<&Vertex<V>> {
        self.vertices.get(index.0)
    }

    /// Returns a mutable reference of a vertex corresponding with the given index, if it exists
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::HyperGraph;
    ///
    /// let mut hg = HyperGraph::new();
    /// let v1 = hg.add_vertex("v1");
    /// let he1 = hg.add_hyper_edge([v1].into(), "he1").unwrap();
    /// assert!(hg.get_vertex(&v1).is_some());
    /// assert_eq!(hg.get_vertex(&v1).unwrap().data, "v1");
    /// ```
    pub fn get_mut_vertex(&mut self, index: &VertexIndex) -> Option<&mut Vertex<V>> {
        self.vertices.get_mut(index.0)
    }

    /// Finds an ear in the hypergraph
    ///
    /// if the algorithm finds an edge which follows the conditions of an ear it will return the ear
    /// otherwise it will return `None`
    ///
    /// See [`Ear`] for more info on Ear edges themselves
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::*;
    ///
    /// let hypergraph = HyperGraph::<(),()>::new();
    /// // add some vertices and edges..
    ///
    /// if let Some(ear) = hypergraph.ear(){
    ///    // do something with ear...
    /// }
    /// // no ear
    /// ```
    #[deprecated = "This runs in O(n^3), use [`ear`]"]
    pub fn ear_bad(&self) -> Option<Ear<'_, HE>> {
        if self.edges.len() == 1 {
            let (_, ear) = self.edges.iter().next().unwrap();
            return Some(Ear::new(ear, None));
        }

        let is_unique_vertex = |x| self.get_vertex_edges(x).len() == 1;
        for (idx, ear_edge) in self.edges.iter() {
            // quick check for isolated edge
            // Do we do this before checking all edges or after?
            if ear_edge.vertices.iter().all(is_unique_vertex) {
                return Some(Ear::new(ear_edge, None));
            }

            // don't check ear on itself
            for (_, parent_edge) in self.edges.iter().filter(|(i, _)| idx != *i) {
                if ear_edge
                    .vertices
                    .difference(&parent_edge.vertices)
                    .all(is_unique_vertex)
                {
                    return Some(Ear::new(ear_edge, Some(parent_edge)));
                }
            }
        }
        None
    }

    /// Finds an ear in the hypergraph
    ///
    /// if the algorithm finds an edge which follows the conditions of an ear it will return the ear
    /// otherwise it will return `None`
    ///
    /// See [`Ear`] for more info on Ear edges themselves
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::*;
    ///
    /// let hypergraph = HyperGraph::<(),()>::new();
    /// // add some vertices and edges..
    ///
    /// if let Some(ear) = hypergraph.ear(){
    ///    // do something with ear...
    /// }
    /// // no ear
    /// ```
    pub fn ear(&self) -> Option<Ear<'_, HE>> {
        for (possible_ear_idx, possible_ear) in self.edges.iter() {
            let possible_parents = possible_ear
                .vertices
                .iter()
                .map(|vi| self.get_vertex_edges(vi))
                .filter(|&vs| vs.len() > 1) // if vs.len() == 1 than that vertex is unique to the ear, `vs` doesn't matter in this case
                .cloned()
                .reduce(|acc, edges| acc.intersection(&edges).copied().collect());

            match possible_parents {
                Some(parents) => {
                    if let Some(parent) = parents.iter().find(|he| he.0 != possible_ear_idx) {
                        return Some(Ear::new(possible_ear, self.edges.get(parent.0)));
                    }
                }
                // the filter above removes every vertex that is unique to possible_ear
                // so if `reduce` returns `None` we know that the iterator was empty and therefore
                // possible_ear is an ear with no direct parent a.k.a. an isolated edge
                None => return Some(Ear::new(possible_ear, None)),
            };
        }
        None
    }

    /// Checks wether this Hypergraph is 𝛼-acyclic using the [GYO-reduction algorithm](https://en.wikipedia.org/wiki/GYO_algorithm)
    ///
    /// At this stage the algorithm still does a clone of the hypergraph because of the algorithm
    /// maybe in the future a shallow Hypergraph can be made to make this clone low-cost
    ///
    /// Empty Graphs or Graphs with only 1 edge are considered 𝛼-acyclic
    /// # Example
    ///```
    /// use hyper::hypergraph::*;
    ///
    /// let hg = HyperGraph::<(),()>::new();
    /// if hg.is_acyclic(){
    ///     // do something if acyclic
    /// } else{}
    ///
    ///```
    ///
    pub fn is_acyclic(&self) -> bool {
        let mut hg_clone = self.clone();
        while let Some(ear) = hg_clone.ear() {
            let ear_key = HyperEdgeIndex(hg_clone.edges.key_of(ear.ear));
            hg_clone.remove_ear(&ear_key);
        }
        hg_clone.is_empty()
    }

    /// Helper that removes an ear and every vertex that is only in that ear
    fn remove_ear(&mut self, ear_key: &HyperEdgeIndex) {
        // Iterate over the vertices and remove if unique
        let ear = self.remove_hyper_edge(*ear_key);
        for v_i in ear.unwrap().vertices {
            let edges_of_vertex = self.get_vertex_edges(&v_i);
            // we just removed the ear edge, so if a vertex is now isolated, we remove it
            if edges_of_vertex.is_empty() {
                self.remove_vertex(v_i);
            }
        }
    }

    /// Performs the same algorithm as [`is_acyclic`](HyperGraph::is_acyclic) but instead
    /// it consumes self and creates a [JoinTree] out of the Hypergraph
    ///
    /// If the Hypergraph is acyclic the method will return the `JoinTree` otherwise `None`
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::*;
    ///
    /// let mut hg = HyperGraph::new();
    /// let p = hg.add_vertex("p");
    /// let q = hg.add_vertex("q");
    /// let w = hg.add_vertex("w");
    /// let x = hg.add_vertex("x");
    /// let y = hg.add_vertex("y");
    /// let z = hg.add_vertex("z");
    ///
    /// let _a = hg.add_hyper_edge([p, y].into(), "a");
    /// let _c = hg.add_hyper_edge([p, y, x].into(), "c");
    /// let _b = hg.add_hyper_edge([x, q].into(), "b");
    /// let _d = hg.add_hyper_edge([w, y, z].into(), "d");
    /// let _e = hg.add_hyper_edge([x, y, z].into(), "e");
    ///
    /// assert!(hg.into_join_tree().is_some());
    /// ```
    pub fn into_join_tree(self) -> Option<JoinTree<V, HE>> {
        self.into_join_tree_with(|edge, _vertices| JoinTreeNode::new(edge.data.clone()))
    }

    /// This is exactly the same as [`into_join_tree`], but you can give a function that creates your own
    /// contained data for the nodes of the join tree
    ///
    /// The function gives you the edge and it's vertices and you can choose how to create the contained data
    /// for the nodes
    ///
    /// # Example
    /// ```
    /// use hyper::hypergraph::*;
    ///
    /// let mut hg = HyperGraph::new();
    /// let p = hg.add_vertex("p");
    /// let q = hg.add_vertex("q");
    /// let w = hg.add_vertex("w");
    /// let x = hg.add_vertex("x");
    /// let y = hg.add_vertex("y");
    /// let z = hg.add_vertex("z");
    ///
    /// let _a = hg.add_hyper_edge([p, y].into(), "a");
    /// let _c = hg.add_hyper_edge([p, y, x].into(), "c");
    /// let _b = hg.add_hyper_edge([x, q].into(), "b");
    /// let _d = hg.add_hyper_edge([w, y, z].into(), "d");
    /// let _e = hg.add_hyper_edge([x, y, z].into(), "e");
    ///

    pub fn into_join_tree_with<F>(mut self, edge_into_node: F) -> Option<JoinTree<V, HE>>
    where
        F: Fn(&HyperEdge<HE>, &mut dyn Iterator<Item = &Vertex<V>>) -> JoinTreeNode<V, HE>,
    {
        // AHash is fast in the case of integer keys, which we have.
        let mut tree: HashMap<_, _, BuildHasherDefault<AHasher>> = Default::default();
        tree.reserve(self.edges.len() / 2); // we don't want to keep allocating in the beginning
        while let Some(ear) = self.ear() {
            // We gotta check what for ear this is
            let ear_key = HyperEdgeIndex(self.edges.key_of(ear.ear));
            let (ear, parent) = match ear {
                Ear {
                    ear,
                    parent: Some(parent),
                } => (ear, parent),
                Ear { ear, parent: None } if self.edges.len() > 1 => (
                    ear,
                    // take the next edge that is different from 'ear' as it's parent
                    self.edges
                        .iter()
                        .find(|&(id, _)| id != ear_key.0)
                        .unwrap()
                        .1,
                ),
                _ => {
                    // the ear is the last edge of this hypergraph
                    self.remove_ear(&ear_key);
                    break;
                } // we got all the cases
            };

            // so we have the ear
            // check if this ear was a parent in previous iterations
            let ear_node = tree.remove(&ear_key).unwrap_or_else(|| {
                edge_into_node(
                    ear,
                    &mut ear
                        .vertices
                        .iter()
                        .map(|v_i| self.vertices.get(v_i.0).unwrap()),
                )
            });

            // we have to find the parent if it already existed
            tree.entry(HyperEdgeIndex(self.edges.key_of(parent)))
                .or_insert_with(|| {
                    edge_into_node(
                        parent,
                        &mut parent
                            .vertices
                            .iter()
                            .map(|v_i| self.vertices.get(v_i.0).unwrap()),
                    )
                })
                .add_child(
                    ear_node,
                    ear.vertices
                        .intersection(&parent.vertices)
                        .filter_map(|v_i| self.get_vertex(v_i).map(|v| v.data.clone()))
                        .collect(),
                );

            self.remove_ear(&ear_key);
        }

        // if the tree has multiple roots, than the join tree is not connected and thus not valid
        if self.is_empty() && tree.len() == 1 {
            // unwrap is safe because the `if` checks the len
            let temp = tree.into_iter().next().unwrap().1;
            Some(JoinTree::with_root(temp))
        } else {
            None
        }
    }

    pub fn edges_to_edges_graphviz(&self) -> impl Display {
        let (vertices, edges) = self.edges.iter().fold(
            (String::new(), String::new()),
            |(mut v, mut e), (i, edge)| {
                let edges_of_edge = edge
                    .vertices
                    .iter()
                    .flat_map(|v_i| self.get_vertex_edges(v_i).iter().map(|he_i| he_i.0))
                    .filter(|&outgoing| i != outgoing)
                    .fold(String::new(), |mut acc, outgoing| {
                        let _ = writeln!(
                            acc,
                            "{i} -> {outgoing}[arrowhead=normal, arrowtail=none, dir=none]"
                        );
                        acc
                    });
                let _ = writeln!(
                    v,
                    "{i}[shape=box label = {:?}]",
                    format!("{:?}", edge.data).replace("\"\"", "\"")
                );
                let _ = writeln!(e, "{edges_of_edge}");
                (v, e)
            },
        );

        let result_graph = format!(
            r#"
digraph{{
    subgraph cluster_1{{
        graph[label = "graph edge to edge"]
        {vertices}
        {edges}
    }}
}}"#,
        );
        result_graph
    }
}
#[cfg(test)]
mod tests {

    use super::HyperGraph;
    const fn count_helper<const N: usize>(_: [(); N]) -> usize {
        N
    }

    macro_rules! replace_expr {
        ($_t:tt, $sub:expr) => {
            $sub
        };
    }

    macro_rules! count_tts {
    ($($smth:tt)*) => {
        count_helper([$(replace_expr!($smth, ())),*])
    }
    }

    macro_rules! hyper_graph {
        (v: {$($v:ident),*}; h: [$($he:ident => {$($vs:ident),*}),*]) => {
            {
                const EDGES_COUNT: usize = count_tts!($($he)*);
                const VERTICES_COUNT: usize = count_tts!($($v)*);
                let mut hg = HyperGraph::with_capacity(EDGES_COUNT, VERTICES_COUNT);
                $(let $v = hg.add_vertex(stringify!($v)));*;

                $(
                    let $he = hg.add_hyper_edge([$($vs),*].into(), stringify!($he));
                )*

                let edges: [_; EDGES_COUNT] = [$($he),*];
                (hg, [$($v),*], edges)
            }
        };

    }
    #[test]
    fn test_ear_2() -> Result<(), &'static str> {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e, f};
            h: [
                t => {f, a, e},
                r => {a, b, c},
                s => {e, c, d},
                u => {a, e, c}
            ]
        };

        assert!(hg.is_acyclic());
        Ok(())
    }

    #[test]
    fn test_find_ear() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e, f};
            h: [
                t => {f, a, e},
                r => {a, b, c},
                s => {e, c, d},
                u => {a, e, c}
            ]
        };
        let ear = hg.ear();

        assert!(ear.is_some());
        assert!(hg.is_acyclic());
    }

    #[test]
    fn test_acyclic() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e, f};
            h: [
                e1 => {a, b, c},
                e2 => {b, c},
                e3 => {c, d},
                e4 => {b, e, f}
            ]
        };

        assert!(hg.is_acyclic(), "it is acyclic");
    }
    #[test]
    fn test_cyclic() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c};
            h: [
                e1 => {a, b},
                e2 => {b, c},
                e3 => {c, a}
            ]
        };

        assert!(!hg.is_acyclic(), "this is cyclic");
    }

    #[test]
    fn test_acyclic_with_special_edge() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c};
            h: [
                e1 => {a, b},
                e2 => {b, c},
                e3 => {c, a},
                e4 => {a, b, c}
            ]
        };

        assert!(hg.is_acyclic(), "this is acyclic");
    }

    #[test]
    fn test_example_presentatie() {
        let (hg, ..) = hyper_graph! {
            v: {u, n, p, o, r, toilet};
            h: [
                users => {u, n, p},
                products => {p, r, toilet},
                orders => {o, u, p}
            ]
        };

        assert!(hg.is_acyclic(), "this is acyclic");
    }

    #[test]
    fn test_isolated() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e};
            h: [
                e1 => {a, b},
                e2 => {b, c},
                e3 => {c, a},
                e4 => {a, b, c},
                isolated => {d, e}
            ]
        };

        assert!(hg.is_acyclic(), "this is acyclic");
    }

    #[test]
    fn test_cyclic_with_isolated() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e};
            h: [
                e1 => {a, b},
                e2 => {b, c},
                e3 => {c, a},
                isolated => {d, e}
            ]
        };

        assert!(!hg.is_acyclic(), "this is cyclic");
    }

    #[test]
    pub fn test_acyclic_from_aayushacharya() {
        let (hg, ..) = hyper_graph! {
            v: {p, q, w, x, y, z};
            h: [
                a => {p, y},
                b => {x, q},
                c => {p, y, x},
                d => {w, y, z},
                e => {x, y, z}
            ]
        };

        println!("{hg:#?}");
        assert!(hg.is_acyclic(), "it is acyclic");
    }

    #[test]
    /// Answer():- A(x, y, z), B(z, w), C(x, w)
    fn test_weird_triangle() {
        let (hg, ..) = hyper_graph! {
            v: {w, x, y, z};
            h: [
                a => {x, y, z},
                b => {z, w},
                c => {x, w}
            ]
        };

        assert!(!hg.is_acyclic());
    }

    #[test]
    fn test_flower() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e};
            h: [
                x => {a, d },
                y => {b, d},
                z => {c, d},
                w => {d, e}
            ]
        };

        assert!(hg.is_acyclic());
    }
    #[test]
    fn test_cyclic_flower() {
        let (hg, ..) = hyper_graph! {
            v: {a, b, c, d, e};
            h: [
                x => {a, d},
                y => {b, d},
                z => {c, d},
                w => {d, e},
                cycle => {a, b}
            ]
        };

        assert!(!hg.is_acyclic());
    }

    #[test]
    fn test_from_nested() {
        let (hg, ..) = hyper_graph! {
            v: {a, b};
            h: [
                y => {a},
                z => {b},
                x => {a, b}
            ]
        };
        dbg!(hg.into_join_tree().unwrap());
    }
}
