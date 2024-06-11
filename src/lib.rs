pub mod equivalence_list;
pub mod execution_planner;
pub mod hyper;
pub mod node;
pub mod optimizer;
/// These tests try to test a lot of different queries to different schemas
/// and see if the optimizer and the node can handle them
#[cfg(test)]
#[allow(clippy::all)]
mod tests {

    use std::sync::Arc;

    use tokio::sync::Mutex;

    use datafusion::common::Result;
    use datafusion::logical_expr::{Join, JoinType, LogicalPlan};
    use datafusion::optimizer::OptimizerRule;
    use datafusion::{
        error::DataFusionError,
        prelude::{CsvReadOptions, SessionContext},
    };
    use futures::future::join_all;
    use tokio::try_join;

    use datafusion::logical_expr::UserDefinedLogicalNode;

    use crate::node::AcyclicJoin;
    use crate::optimizer::AcyclicJoinOptimizerRule;

    use itertools::*;

    const OPT: AcyclicJoinOptimizerRule = AcyclicJoinOptimizerRule {};

    pub enum TestResult {
        Optimized {
            original: LogicalPlan,
            optimized: LogicalPlan,
        },
        UnOptimized(LogicalPlan),
    }

    impl TestResult {
        pub fn is_optimized(&self) -> bool {
            matches!(self, TestResult::Optimized { .. })
        }

        pub fn is_unoptimized(&self) -> bool {
            matches!(self, TestResult::UnOptimized(_))
        }
    }

    fn find_acyclicjoin_node(plan: &LogicalPlan) -> Option<&AcyclicJoin> {
        if let LogicalPlan::Extension(extension) = plan {
            return (*extension.node).as_any().downcast_ref();
        }
        for child in plan.inputs() {
            if let Some(node) = find_acyclicjoin_node(child) {
                return Some(node);
            }
        }
        None
    }

    fn find_acyclicjoin_nodes(plan: &LogicalPlan) -> Vec<&AcyclicJoin> {
        let mut nodes = Vec::new();
        if let LogicalPlan::Extension(extension) = plan {
            nodes.push((*extension.node).as_any().downcast_ref().unwrap());
        }
        for child in plan.inputs() {
            nodes.extend(find_acyclicjoin_nodes(child));
        }
        nodes
    }

    fn count_joins(plan: &LogicalPlan) -> usize {
        let mut sum = 0;
        if let LogicalPlan::Join(Join {
            join_type: JoinType::Inner,
            ..
        }) = plan
        {
            sum += 1;
        }

        for child in plan.inputs() {
            sum += count_joins(child);
        }

        sum
    }

    async fn test_schema_and_query(
        schema: &str,
        query: &str,
    ) -> Result<TestResult, DataFusionError> {
        let ctx = Arc::new(Mutex::new(SessionContext::new()));

        let mut results = vec![];
        for query in schema
            .split(';')
            .map(|s| String::from(s.trim()))
            .filter(|s| !s.is_empty())
        // .inspect(|s| eprintln!("query: {s}"))
        {
            let ctx = Arc::clone(&ctx);
            results.push(tokio::spawn(async move {
                ctx.lock()
                    .await
                    .sql(&query)
                    .await
                    .expect(&format!("sql create table: {query} should not fail"))
                    .execute_stream()
                    .await
                    .expect("sql create table should run");
            }));
        }

        let _ = join_all(results).await;

        // 1 query from tpch has 3 statements for a view, this is for that...
        let mut query_statements = query.trim_end().split_inclusive(';');

        let query = query_statements
            .next_back()
            .expect("there should be at least one statement in a given query");

        let _results = join_all(
            query_statements
                .map(|s| async { ctx.lock().await.sql(s).await?.execute_stream().await }),
        )
        .await;

        let df = ctx.lock().await.sql(query).await?;
        let original = df.clone().into_optimized_plan()?;
        // dbg!(df.clone().into_unoptimized_plan());
        let state = ctx.lock().await.state();

        Ok(match OPT.try_optimize(&original, &state)? {
            Some(optimized) => TestResult::Optimized {
                original,
                optimized,
            },
            None => TestResult::UnOptimized(original),
        })
    }
    #[tokio::test]
    async fn users_orders_products() -> Result<()> {
        let ctx = SessionContext::new();

        let csv_options = CsvReadOptions::new();
        let customers = ctx.register_csv("users", "data/customer.csv", csv_options.clone());
        let products = ctx.register_csv("products", "data/products.csv", csv_options.clone());
        let orders = ctx.register_csv("orders", "data/orders.csv", csv_options.clone());

        try_join!(customers, products, orders)?;

        // create a plan
        // answer(n, u_id, o_id) =
        //     Users(u_id, u_name), Products(p_id, 'toilet'), Orders(u_id, p_id, o_id)
        let df = ctx
            .sql(
                "SELECT users.name, users.uid, orders.oid
                FROM users, products, orders
                WHERE users.uid = orders.uid
                    and products.pid = orders.pid
                    and products.name = 'Toilet'",
            )
            .await?;

        // try_optimize returns Some if it succesfully optimizes
        let optimized = OPT.try_optimize(&df.into_optimized_plan()?, &ctx.state())?;
        match optimized {
            // maybe create some fancy checker that it contains an acyclic join with the right inputs
            Some(_plan) => {}
            None => panic!("Query should have an acyclicjoin"),
        };
        Ok(())
    }

    // The inner inner-join can be optimized
    #[tokio::test]
    async fn triangle_join() -> Result<()> {
        let result = test_schema_and_query(
            include_str!("../schemas/cyclic_schema.sql"),
            include_str!("../bench_queries/cyclic_join.sql"),
        )
        .await?;

        if let TestResult::UnOptimized(input) = result {
            panic!("Following query with logicalplan:\n {input:?}\nshould have been optimized but wasn't");
        }
        Ok(())
    }

    // The inner inner-join can be optimized
    #[tokio::test]
    async fn weird_triangle() -> Result<()> {
        let result = test_schema_and_query(
            include_str!("../schemas/weird_triangle_schema.sql"),
            include_str!("../bench_queries/weird_triangle_join.sql"),
        )
        .await?;
        if let TestResult::UnOptimized(input) = result {
            panic!("Following query with logicalplan:\n {input:?}\nshould have been optimized but wasn't");
        }
        Ok(())
    }

    #[tokio::test]
    async fn flower() -> Result<()> {
        let result = test_schema_and_query(
            include_str!("../schemas/flower.sql"),
            include_str!("../bench_queries/flower.sql"),
        )
        .await?;

        assert!(result.is_optimized(), "Flower Query is an acyclic join");
        Ok(())
    }

    #[macro_export]
    macro_rules! test_query_to_schema {
        ($query_number:expr => $schema_path:literal; $query_path:literal) => {
            test_schema_and_query(
                include_str!($schema_path),
                include_str!(concat!(
                    "../bench_queries/",
                    $query_path,
                    "/",
                    stringify!($query_number),
                    ".sql"
                )),
            )
            .await?
        };
    }

    #[macro_export]
    macro_rules! test_fn_impl {
        ($query_n:expr => optimizable; schema = $schema_path:literal, query_path = $query_path:literal) => {
            paste::paste! {
                #[tokio::test]
                async fn [<query_ $query_n _is_optimizable>]() -> Result<(), DataFusionError> {
                    let result = test_query_to_schema!($query_n => $schema_path; $query_path);
                    match result{
                        TestResult::UnOptimized(input) => panic!("Following query with logicalplan:\n {input:?}\nshould have been optimized but wasn't"),
                        TestResult::Optimized{optimized, original} => {
                            let join_amount = count_joins(&original) ; // a join can be an input to 1 other join, so we do +1 to get last input
                            let acj_inputs = find_acyclicjoin_nodes(&optimized).iter().map(|acj| acj.inputs().len()).collect_vec();
                            eprintln!("query {} has {} joins, which resulted in following acyclicjoin joins: {:?}", stringify!($query_n), join_amount, acj_inputs);
                        }
                        // _ => {}
                    };
                    Ok(())
                }
            }
        };
        ($query_n:expr => unoptimizable; schema = $schema_path:literal, query_path = $query_path:literal) => {
            paste::paste! {
                #[tokio::test]
                async fn [<query_ $query_n:lower _is_unoptimizable>]() -> Result<(), DataFusionError> {
                    let result = test_query_to_schema!($query_n => $schema_path; $query_path);
                    if let TestResult::Optimized{original, optimized} = result{
                        panic!("Following query with logicalPlan:\n\
                                {original:?}\n\
                                was optimized to:\n\
                                {optimized:?}\n\
                                but should not be optimizable with AcyclicJoins");
                    }
                    Ok(())
                }
            }
        };
        ($query_n:expr => binary; schema = $schema_path:literal, query_path = $query_path:literal) => {
            paste::paste! {
                #[ignore = "Don't know how to handle binary joins yet."]
                #[tokio::test]
                async fn [<query_ $query_n:lower _has_binary_join>]() -> Result<(), DataFusionError> {
                    let result = test_query_to_schema!($query_n => $schema_path; $query_path);
                    if let TestResult::Optimized{optimized, ..} = result{
                        let acyclic_join = find_acyclicjoin_node(&optimized).unwrap();
                        assert!(acyclic_join.inputs().len() == 2, "Expected Binary join but got following inputs instead:\n{:?}", acyclic_join.inputs());
                    }
                    Ok(())
                }
            }
        };
        ($query_n:expr => ignore;  schema = $schema_path:literal, query_path = $query_path:literal) => {
            paste::paste! {
                #[ignore = ""]
                #[tokio::test]
                async fn [<query_ $query_n:lower _has_binary_join>]() -> Result<(), DataFusionError> {
                    let result = test_query_to_schema!($query_n => $schema_path; $query_path);
                    if let TestResult::Optimized{optimized, ..} = result{
                        let acyclic_join = find_acyclicjoin_node(&optimized).unwrap();
                        assert!(acyclic_join.inputs().len() == 2, "Expected Binary join but got following inputs instead:\n{:?}", acyclic_join.inputs());
                    }
                    Ok(())
                }
            }
        };
    }

    // Test the IMDB benchmark queries
    // All 33 queries have acyclic joins
    #[cfg(test)]
    mod imdb {
        use super::*;
        macro_rules! test_imdb_query_fn {

            ($($query_n:expr => $kind:ident),* $(,)?) => {
              $(test_fn_impl!($query_n => $kind; schema = "../schemas/imdb_schema.sql", query_path = "imdb");)*
            };

        }

        // All the imdb queries are Acyclic
        test_imdb_query_fn! {
            1a => optimizable,
            2a => optimizable,
            3a => optimizable,
            4a => optimizable,
            5a => optimizable,
            6a => optimizable,
            7a => optimizable,
            8a => optimizable,
            9a => optimizable,
            10a => optimizable,
            11a => optimizable,
            12a => optimizable,
            13a => optimizable,
            14a => optimizable,
            15a => optimizable,
            16a => optimizable,
            17a => optimizable,
            18a => optimizable,
            19a => optimizable,
            20a => optimizable,
            21a => optimizable,
            22a => optimizable,
            23a => optimizable,
            24a => optimizable,
            25a => optimizable,
            26a => optimizable,
            27a => optimizable,
            28a => optimizable,
            29a => optimizable,
            30a => optimizable,
            31a => optimizable,
            32a => optimizable,
            33a => optimizable,
        }
    }

    // Test the TPC-H queries
    // Has 17 acyclic Joins, 5 are/have binary joins
    #[cfg(test)]
    mod tpc_h {
        use super::*;
        macro_rules! test_tpch_query_fn {
            ($($query_n:expr => $kind:ident),* $(,)?) => {
              $(test_fn_impl!($query_n => $kind; schema = "../bench_queries/tpc-h/schema.ddl", query_path = "tpc-h/queries");)*
            };

        }

        test_tpch_query_fn! {
            // checked
            q1 => unoptimizable,

            // checked, not hard to check
            q2 => optimizable,

            // checked, not hard to check
            q3 => optimizable,

            // checked => no joins
            q4 => unoptimizable,

            // checked, cyclic because of the joins on
            // custumer.c_nationkey = supplier.s_nationkey
            // custumer.c_custkey = orders.o_custkey
            // orders.o_orderkey = lineitem.l_orderkey
            // lineitem.l_suppkey = suppliers.s_suppkey

            // we join something like this: (can start on any input)
            // Customer -> Supplier -> LineItem -> Orders
            // ^--------------------------------------|
            // that is a cycle
            // BUT there is a join in the join-tree that is acyclic, so it is optimizable
            q5 => optimizable,

            // checked => no joins
            q6 => unoptimizable,

            // checked => Querygraph is "linear"
            // Nation1 <-> Customer <-> Orders <-> LineItem <-> Supplier <-> Nation2
            q7 => optimizable,

            // checked => manually Created Hypergraph with Join Tree just to be sure
            q8 => optimizable,

            // checked mannually
            q9 => optimizable,


            // checked mannually
            q10 => optimizable,

            // checked mannually
            // has 3 acyclicjoins
            q11 => optimizable,

            // checked, join of 2 tables on 1 column
            q12 => binary,

            // checked, is a left outer join
            q13 => unoptimizable,

            // checked join of 2 tables
            q14 => binary,

            // checked mannualy
            q15 => optimizable,

            // checked, join of 2 tables
            q16 => binary,

            // checked, join of 2 tables
            q17 => binary,

            // checked
            q18 => optimizable,

            // join of 2 tables on 1 column, with gigantic filters
            q19 => binary,

            q20 => binary,

            q21 => optimizable,

            // no join
            q22 => unoptimizable,
        }
    }

    // Has 97 acyclic joins, 6 are/have binary joins
    #[cfg(test)]
    mod tpc_ds {
        use super::*;
        macro_rules! test_tpcds_query_fn {
            ($($query_n:expr => $kind:ident),* $(,)?) => {
              $(test_fn_impl!($query_n => $kind ; schema = "../bench_queries/tpc-ds/schema.sql", query_path = "tpc-ds/queries");)*
            };

        }

        test_tpcds_query_fn! {
            q1 => optimizable,
            q2 => binary,
            q3 => optimizable,
            q4 => optimizable,
            q5 => optimizable,
            q6 => optimizable,
            q7 => optimizable,
            q8 => optimizable,

            // not a join in sight
            q9 => unoptimizable,
            q10 => optimizable,
            q11 => optimizable,
            q12 => optimizable,
            q13 => unoptimizable,
            q14 => optimizable,
            q15 => unoptimizable,
            q16 => optimizable,
            q17 => optimizable,
            q18 => optimizable,
            q19 => optimizable,
            q20 => optimizable,

            // fails with a syntax error i can't find
            // q21 => optimizable,
            q22 => optimizable,
            q23 => optimizable,

            // very complex query, don't know if my check is faulty
            // or there isn't a acyclic join
            // acyclicjoin is very deep
            q24 => optimizable,
            q25 => optimizable,
            q26 => optimizable,
            q27 => optimizable,

            // not a join in sight
            q28 => unoptimizable,
            q29 => optimizable,
            q30 => optimizable,
            q31 => optimizable,
            q32 => optimizable,
            q33 => optimizable,
            q34 => optimizable,
            q35 => optimizable,
            q36 => optimizable,
            q37 => optimizable,
            q38 => optimizable,
            q39 => optimizable,
            q40 => optimizable,

            // fails with:
            // Correlated column is not allowed in predicate
            q41 => ignore,
            q42 => optimizable,
            q43 => optimizable,
            q44 => optimizable,
            q45 => optimizable,
            q46 => optimizable,
            q47 => optimizable,
            q48 => unoptimizable,
            q49 => binary,
            q50 => optimizable,
            q51 => binary,
            q52 => optimizable,
            q53 => optimizable,
            q54 => optimizable,
            q55 => optimizable,
            q56 => optimizable,
            q57 => optimizable,
            q58 => optimizable,
            q59 => optimizable,
            q60 => optimizable,
            q61 => optimizable,
            q62 => optimizable,
            q63 => optimizable,
            q64 => optimizable,
            q65 => optimizable,
            q66 => optimizable,
            q67 => optimizable,
            q68 => optimizable,
            q69 => optimizable,
            q70 => optimizable,
            q71 => optimizable,

            // very deep acyclicjoin
            q72 => optimizable,
            q73 => optimizable,
            q74 => optimizable,
            q75 => optimizable,
            q76 => optimizable,
            q77 => optimizable,
            q78 => binary,
            q79 => optimizable,
            q80 => optimizable,
            q81 => optimizable,
            q82 => optimizable,
            q83 => optimizable,
            q84 => optimizable,

            // very deep acyclicjoin
            q85 => optimizable,
            q86 => optimizable,
            q87 => optimizable,
            q88 => optimizable,
            q89 => optimizable,
            q90 => optimizable,
            q91 => optimizable,
            q92 => optimizable,
            q93 => binary,
            q94 => optimizable,
            q95 => optimizable,
            q96 => optimizable,
            q97 => binary,
            q98 => optimizable,
            q99 => optimizable,
        }
    }
}
