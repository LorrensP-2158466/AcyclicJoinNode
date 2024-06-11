use std::sync::Arc;

use datafusion::{
    arrow,
    error::DataFusionError,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    optimizer::push_down_filter::PushDownFilter,
    prelude::{SessionConfig, SessionContext},
};
use futures::future::join_all;

use acyclicjoin_node::{
    execution_planner::AcyclicJoinQueryPlanner, optimizer::AcyclicJoinOptimizerRule,
};

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let config = SessionConfig::default();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime)
        .add_optimizer_rule(Arc::new(PushDownFilter::new()))
        .add_optimizer_rule(Arc::new(AcyclicJoinOptimizerRule {}))
        .with_query_planner(Arc::new(AcyclicJoinQueryPlanner {}));
    let ctx = SessionContext::new_with_state(state);

    join_all(
        include_str!("../../schemas/uop.sql")
            .split(';')
            .map(|s| async { ctx.sql(s).await?.execute_stream().await }),
    )
    .await;

    let _insert_df = ctx
        .sql(include_str!("../../inserts/uop_orders.sql"))
        .await?
        .collect()
        .await?;
    let _insert_df = ctx
        .sql(include_str!("../../inserts/uop_users.sql"))
        .await?
        .collect()
        .await?;
    let _insert_df = ctx
        .sql(include_str!("../../inserts/uop_products.sql"))
        .await?
        .collect()
        .await?;
    let df = ctx.sql(include_str!("../../bench_queries/uop.sql")).await?;

    println!("{}", df.clone().into_unoptimized_plan().display_graphviz());
    println!("{}", df.clone().into_optimized_plan()?.display_graphviz());
    let results = df.collect().await?;

    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?.to_string();

    println!("{pretty_results}");

    // execute the plan
    Ok(())
}
