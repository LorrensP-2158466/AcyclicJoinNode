//! This module defines a struct where you can define your own equivalance classes
use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;

use itertools::*;

/// An EquiClassList defines a collection over `V`'s where we can define which `V`'s are equal to eachother and which are not.
/// See [`EquiClassList::insert`] for more info.
/// This struct is nice for resolving column names of databases
#[derive(Debug, Clone)]
pub struct EquiClassList<V> {
    vals: Vec<HashSet<V>>,
}

impl<V> EquiClassList<V>
where
    V: Eq + PartialEq + Hash + Clone,
{
    /// Contruct a empty EquiLis
    pub fn new() -> Self {
        Self { vals: Vec::new() }
    }

    /// Check's if the value is in any EquiClass
    #[allow(unused)]
    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        V: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.vals.iter().any(|set| set.contains(value))
    }

    /// Get's the equi-class in which the value is in.
    pub fn get_class<Q>(&self, value: &Q) -> Option<&HashSet<V>>
    // thank you std lib :)
    where
        V: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.vals.iter().find(|set| set.contains(value))
    }

    /// Internal index of the equi-class in which the value is in.
    pub fn get_class_idx<Q>(&self, value: &Q) -> Option<usize>
    // thank you std lib :)
    where
        V: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.vals.iter().position(|set| set.contains(value))
    }

    /// The equivalence map maps sets of Keys to the same value without copying the value.  
    /// But the behaviour is different based on how the keys are equivaleted. There are four scenarios that can
    /// happen when inserting:  
    ///
    ///    1. Both values were not inserted and thus both are in the same equi-class
    ///    2. One of the values was inserted and the other not, the other will be placed in the set of the existing one
    ///    3. Both values are already inserted in the same equi-class, this is a no-op
    ///    4. Both values are already inserted, but in different equi-classes, the equi-classes were these are contained in will be joined together
    ///
    /// The examples will explain the scenarios further:
    /// # Examples
    /// ```
    /// use acyclicjoin_node::equivalence_map::*;
    /// let mut map = EquivalenceMap::new();
    /// map.insert(("hello", "salut"));
    /// assert_eq!(map.get_class_idx("hello"), map.get_class_idx("salut"));
    /// ```
    /// This illustrates scenario 1.
    ///
    /// ```
    /// # use acyclicjoin_node::equivalence_map::*;
    /// let mut map = EquivalenceMap::new();
    /// map.insert(("hello", "salut"));
    /// // we inserted ["hello", "salut"]
    /// map.insert(("hello", "world"));
    ///
    /// // "hello" was already inserted, so now "world" will be inserted in the equi-class of "hello"
    /// assert_eq!(map.get_class_idx("salut"), map.get_class_idx("world"));
    /// ```
    /// This illustrates scenario 2.
    ///
    ///
    /// The next example shows how we resolve column names to be in the same equi-class
    /// ```
    /// use acyclicjoin_node::equivalence_map::*;
    /// let mut resolver = EquivalenceMap::new();
    ///
    /// let common_name = "t.id";
    /// // we create names we want to resolve to the same name ("t.id")
    /// let names = [
    ///     "mi.movie_id",
    ///     "mi_idx.movie_id",
    ///     "ci.movie_id",
    ///     "mk.movie_id",
    /// ];
    ///
    /// for &name in names.iter() {
    ///     resolver.insert((name, common_name));
    /// }
    ///
    /// // we already inserted ci.movie_id and mk.movie_id to the same equi-class
    /// // this will do nothing
    /// resolver.insert(("ci.movie_id", "mk.movie_id"));
    ///
    /// // check every name to every other name to see if they live in the same set
    ///
    /// for &name_1 in names.iter() {
    ///     for &name_2 in names.iter() {
    ///         assert_eq!(resolver.get_class_idx(name_1), resolver.get_class_idx(name_2));
    ///     }
    /// }
    /// ```
    /// This illustrates scenario 3.
    ///
    /// ```
    /// # use acyclicjoin_node::equivalence_map::*;
    /// let mut map = EquivalenceMap::new();
    ///
    /// map.insert(("hello", "world"));
    ///
    /// map.insert(("no way", "goodbye");
    ///
    /// map.insert(("hello", "goodbye"));
    ///
    /// // the equi-classes of "hello" and "goodbye" are joined
    /// assert_eq!(map.get_class_idx("hello"), map.get_class_idx("goodbye"));
    /// ```
    /// This illustrates scenario 4.
    pub fn insert(&mut self, (key1, key2): (V, V)) {
        let find_keyset = |key| self.vals.iter().position(|set| set.contains(key));
        match (find_keyset(&key1), find_keyset(&key2)) {
            // one of the names is found
            (Some(i), None) | (None, Some(i)) => {
                let set = &mut self.vals[i];
                set.insert(key1.clone());
                set.insert(key2);
            }
            // only re-resolve if the sets are not the same one
            (Some(l_i), Some(r_i)) if l_i != r_i => {
                let (removed_set, keeped_set) = if self.vals[l_i].len() < self.vals[r_i].len() {
                    let left = self.vals.remove(l_i);
                    let right = self.vals.get_mut(r_i).unwrap();
                    (left, right)
                } else {
                    let right = self.vals.remove(r_i);
                    let left = self.vals.get_mut(l_i).unwrap();
                    (right, left)
                };
                keeped_set.extend(removed_set);
            }
            (None, None) => self.vals.push([key1, key2].into()),
            (Some(_), Some(_)) => {}
        }
    }

    /// Returns an iterator over equi-classes
    pub fn iter(&self) -> impl Iterator<Item = &'_ HashSet<V>> {
        self.vals.iter()
    }

    #[deprecated]
    #[allow(unreachable_code)]
    pub fn insert_multiple(&mut self, _keys: &[V]) {
        unimplemented!("EquiClassList::insert_multiple is not yet supported");
        let key_len = _keys.len();
        let key_set: HashSet<_> = _keys.iter().cloned().collect();

        // we find every equi-class which already contained one of the keys
        let equivalent_keys = self
            .vals
            .iter()
            .enumerate()
            .filter_map(|(i, set)| key_set.intersection(set).next().map(|_| i))
            .sorted_unstable()
            .dedup()
            .collect_vec();
        match equivalent_keys.len() {
            2.. => {
                let mut joined = self.vals.pop().unwrap();

                self.vals.push(joined.clone());
            }
            // N unique keys?
            n if n == key_len => {
                self.vals = _keys
                    .iter()
                    .cloned()
                    .map(|key| HashSet::from([key]))
                    .collect();
            }
            // no key was mapped
            0usize => {
                self.vals.push(key_set);
            }

            // so we don't have N uniques
            // do we have N same keys
            // or just one found key?
            1usize => {
                let set = &mut self.vals[equivalent_keys[0]];
                set.extend(key_set);
            }
        }
    }
}

impl<K> FromIterator<(K, K)> for EquiClassList<K>
where
    K: Clone + Hash + Eq,
{
    fn from_iter<T: IntoIterator<Item = (K, K)>>(iter: T) -> Self {
        let mut result = Self::new();
        for (left, right) in iter.into_iter() {
            result.insert((left, right));
        }
        result
    }
}

impl<N> Default for EquiClassList<N>
where
    N: Eq + PartialEq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::EquiClassList;

    #[test]
    fn test_1_resolve() {
        let mut resolver = EquiClassList::new();
        resolver.insert(("hello", "world"));

        assert_eq!(resolver.get_class("hello"), resolver.get_class("world"));
    }

    #[test]
    fn test_follow_up() {
        let mut resolver = EquiClassList::new();

        // "hello" -> "greeting"
        // "world" -> "greeing"
        resolver.insert(("hello", "world"));

        // world is already resolved
        // "goodbye" -> "greeting"
        resolver.insert(("world", "goodbye"));

        assert_eq!(resolver.get_class("hello"), resolver.get_class("goodbye"));
    }

    #[test]
    fn does_work() {
        let mut resolver = EquiClassList::new();

        // "hello" -> "greeting"
        // "world" -> "greeing"
        resolver.insert(("hello", "world"));

        // world is already resolved
        // "goodbye" -> "greeting"
        resolver.insert(("no way", "goodbye"));

        // "hello" -> "switch"
        // "world" -> "switch"
        // "goodbye" -> "switch"
        // "no way" -> "switch"
        resolver.insert(("hello", "goodbye"));

        assert_eq!(resolver.get_class("hello"), resolver.get_class("goodbye"));
    }

    #[test]
    fn imdb_fail() {
        let mut resolver = EquiClassList::new();

        let common_name = "t.id";
        // we create names we want to resolve to the same name ("t.id")
        let names = [
            "mi.movie_id",
            "mi_idx.movie_id",
            "ci.movie_id",
            "mk.movie_id",
        ];

        for &name in names.iter() {
            resolver.insert((name, common_name));
        }

        //we already resolved ci.movie_id and mk.movie_id to vertex 1
        // this resolve should not create a new index because both live in the same
        // collection
        resolver.insert(("ci.movie_id", "mk.movie_id"));

        // check every name to every other name to see if they live in the same
        // collection

        for &name_1 in names.iter() {
            for &name_2 in names.iter() {
                assert_eq!(resolver.get_class(name_1), resolver.get_class(name_2));
            }
        }
    }
}
