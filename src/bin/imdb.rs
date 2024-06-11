// this executes every bench query of imdb
// every query is acyclic

use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    physical_plan::displayable,
    prelude::{SessionConfig, SessionContext},
};

use futures::future::join_all;

use acyclicjoin_node::{
    execution_planner::AcyclicJoinQueryPlanner, optimizer::AcyclicJoinOptimizerRule,
};

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let config = SessionConfig::new().with_target_partitions(48);
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(config, runtime)
        .with_query_planner(Arc::new(AcyclicJoinQueryPlanner {}))
        .add_optimizer_rule(Arc::new(AcyclicJoinOptimizerRule {}));
    let ctx = SessionContext::new_with_state(state);

    let _results = join_all(
        include_str!("../../schemas/imdb_schema.sql")
            .split(';')
            .map(|s| async { ctx.sql(s).await?.execute_stream().await }),
    );

    _results.await;
    let ctx = Arc::new(ctx);

    join_all(
        std::fs::read_dir("bench_queries/imdb")
            .unwrap()
            .filter_map(|q| q.ok())
            .map(|query| {
                dbg!(&query);
                let ctx = Arc::clone(&ctx);
                tokio::spawn(async move {
                    let query_str = std::fs::read_to_string(query.path()).unwrap();

                    let plan = ctx
                        .sql(&query_str)
                        .await?
                        .create_physical_plan()
                        .await
                        .unwrap();
                    // println!(
                    //     "{}",
                    //     ctx.sql(&query_str)
                    //         .await?
                    //         .into_optimized_plan()?
                    //         .display_graphviz()
                    // );
                    println!("{}", displayable(plan.as_ref()).graphviz());

                    // match OPT.try_optimize(&plan, &ctx.as_ref().state()) {
                    //     Ok(Some(_)) => {
                    //         println!("query {:?} is acyclic", query.file_name())
                    //     }
                    //     Ok(None) => println!("query {:?} is cylic", query.file_name()),
                    //     Err(err) => {
                    //         println!("error occured in query {:?}: {err}", query.file_name())
                    //     }
                    // };
                    Ok::<(), DataFusionError>(())
                })
            }),
    )
    .await;

    // let v = join_all(
    //     std::fs::read_dir("bench_queries/imdb")
    //         .unwrap()
    //         .filter_map(|q| q.ok())
    //         .map(|query| async {
    //             let file_name = query.file_name().into_string().unwrap();
    //             if file_name != "32a.sql" {
    //                 return (file_name, None);
    //             }
    //             let ctx = Arc::clone(&ctx);
    //             let handle = tokio::spawn(async move {
    //                 let query_str = std::fs::read_to_string(query.path()).unwrap();

    //                 let plan = ctx
    //                     .sql(&query_str)
    //                     .await?
    //                     .create_physical_plan()
    //                     .await
    //                     .unwrap();
    //                 // println!(
    //                 //     "{}",
    //                 //     ctx.sql(&query_str)
    //                 //         .await?
    //                 //         .into_optimized_plan()?
    //                 //         .display_graphviz()
    //                 // );
    //                 println!("{}", displayable(plan.as_ref()).graphviz());

    //                 // match OPT.try_optimize(&plan, &ctx.as_ref().state()) {
    //                 //     Ok(Some(_)) => {
    //                 //         println!("query {:?} is acyclic", query.file_name())
    //                 //     }
    //                 //     Ok(None) => println!("query {:?} is cylic", query.file_name()),
    //                 //     Err(err) => {
    //                 //         println!("error occured in query {:?}: {err}", query.file_name())
    //                 //     }
    //                 // };
    //                 Ok::<(), DataFusionError>(())
    //             });
    //             (file_name, Some(handle.await.unwrap_err().into_panic()))
    //         }),
    // )
    // .await;

    // v.into_iter().for_each(|(name, err)| match err {
    //     Some(err) => println!("query {name:?} failed with: {:?}", err.downcast::<&str>()),
    //     None => {}
    // });
    Ok(())
}
