use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};

use futures::future::join_all;

use acyclicjoin_node::optimizer::AcyclicJoinOptimizerRule;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let config = SessionConfig::new().with_target_partitions(48);
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime)
        .add_optimizer_rule(Arc::new(AcyclicJoinOptimizerRule {}));

    let ctx = SessionContext::new_with_state(state);

    let _results = join_all(
        include_str!("../../schemas/weird_triangle_schema.sql")
            .split(';')
            .map(|s| async { ctx.sql(s).await?.execute_stream().await }),
    );

    _results.await;

    let df = ctx
        .sql(include_str!("../../bench_queries/nested_acyclic.sql"))
        .await?;

    println!("{}", df.into_optimized_plan()?.display_graphviz());
    // execute the plan
    Ok(())
}
