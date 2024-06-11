use std::sync::Arc;

use acyclicjoin_node::{
    execution_planner::AcyclicJoinQueryPlanner, optimizer::AcyclicJoinOptimizerRule,
};
use datafusion::{
    error::DataFusionError,
    execution::{config::SessionConfig, context::SessionState, runtime_env::RuntimeEnv},
    physical_plan::displayable,
    prelude::SessionContext,
};
use futures::future::join_all;

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let config = SessionConfig::new().with_target_partitions(48);
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime)
        .with_query_planner(Arc::new(AcyclicJoinQueryPlanner {}))
        .add_optimizer_rule(Arc::new(AcyclicJoinOptimizerRule {}));
    let ctx = SessionContext::new_with_state(state);

    let _results = join_all(
        include_str!("../../schemas/flower.sql")
            .split(';')
            .map(|s| async { ctx.sql(s).await?.execute_stream().await }),
    );

    _results.await;

    let plan = ctx
        .sql(include_str!("../../bench_queries/flower.sql"))
        .await?
        .create_physical_plan()
        .await;
    match plan {
        Ok(plan) => println!("{}", displayable(plan.as_ref()).graphviz()),
        Err(err) => {
            eprintln!("{:#}", err);
        }
    }

    // execute the plan
    Ok(())
}
