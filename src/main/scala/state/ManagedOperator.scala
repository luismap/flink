package state

import org.apache.flink.api.common.state.ListState
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}


/**
 *Operator state is any non-keyed state in Flink. This includes, but is not limited to, any use of
 *CheckpointedFunction or BroadcastState within an application. When reading operator state, users
 * specify the operator uid, the state name, and the type information.
 *
 */
object ManagedOperator {




}
