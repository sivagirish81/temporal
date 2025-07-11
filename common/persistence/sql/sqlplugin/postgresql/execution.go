package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	executionsColumns = `shard_id, namespace_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding, state, state_encoding, db_record_version`

	createExecutionQuery = `INSERT INTO executions(` + executionsColumns + `)
 VALUES(:shard_id, :namespace_id, :workflow_id, :run_id, :next_event_id, :last_write_version, :data, :data_encoding, :state, :state_encoding, :db_record_version)`

	updateExecutionQuery = `UPDATE executions SET
 db_record_version = :db_record_version, next_event_id = :next_event_id, last_write_version = :last_write_version, data = :data, data_encoding = :data_encoding, state = :state, state_encoding = :state_encoding
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getExecutionQuery = `SELECT ` + executionsColumns + ` FROM executions
 WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4`

	deleteExecutionQuery = `DELETE FROM executions 
 WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4`

	lockExecutionQueryBase = `SELECT db_record_version, next_event_id FROM executions 
 WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4`

	writeLockExecutionQuery = lockExecutionQueryBase + ` FOR UPDATE`
	readLockExecutionQuery  = lockExecutionQueryBase + ` FOR SHARE`

	createCurrentExecutionQuery = `INSERT INTO current_executions
(shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, start_time, last_write_version, data, data_encoding) VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :create_request_id, :state, :status, :start_time, :last_write_version, :data, :data_encoding)`

	deleteCurrentExecutionQuery = "DELETE FROM current_executions WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4"

	getCurrentExecutionQuery = `SELECT
shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, start_time, last_write_version, data, data_encoding
FROM current_executions WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3`

	lockCurrentExecutionJoinExecutionsQuery = `SELECT
ce.shard_id, ce.namespace_id, ce.workflow_id, ce.run_id, ce.create_request_id, ce.state, ce.status, ce.start_time, e.last_write_version, ce.data, ce.data_encoding
FROM current_executions ce
INNER JOIN executions e ON e.shard_id = ce.shard_id AND e.namespace_id = ce.namespace_id AND e.workflow_id = ce.workflow_id AND e.run_id = ce.run_id
WHERE ce.shard_id = $1 AND ce.namespace_id = $2 AND ce.workflow_id = $3 FOR UPDATE`

	lockCurrentExecutionQuery = getCurrentExecutionQuery + ` FOR UPDATE`

	updateCurrentExecutionsQuery = `UPDATE current_executions SET
run_id = :run_id,
create_request_id = :create_request_id,
state = :state,
status = :status,
start_time = :start_time,
last_write_version = :last_write_version,
data = :data,
data_encoding = :data_encoding
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id
`

	createHistoryImmediateTasksQuery = `INSERT INTO history_immediate_tasks(shard_id, category_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :category_id, :task_id, :data, :data_encoding)`

	getHistoryImmediateTasksQuery = `SELECT task_id, data, data_encoding 
 FROM history_immediate_tasks WHERE shard_id = $1 AND category_id = $2 AND task_id >= $3 AND task_id < $4 ORDER BY task_id LIMIT $5`

	deleteHistoryImmediateTaskQuery       = `DELETE FROM history_immediate_tasks WHERE shard_id = $1 AND category_id = $2 AND task_id = $3`
	rangeDeleteHistoryImmediateTasksQuery = `DELETE FROM history_immediate_tasks WHERE shard_id = $1 AND category_id = $2 AND task_id >= $3 AND task_id < $4`

	createHistoryScheduledTasksQuery = `INSERT INTO history_scheduled_tasks (shard_id, category_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :category_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getHistoryScheduledTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM history_scheduled_tasks 
  WHERE shard_id = $1 
  AND category_id = $2 
  AND ((visibility_timestamp >= $3 AND task_id >= $4) OR visibility_timestamp > $5) 
  AND visibility_timestamp < $6
  ORDER BY visibility_timestamp,task_id LIMIT $7`

	deleteHistoryScheduledTaskQuery       = `DELETE FROM history_scheduled_tasks WHERE shard_id = $1 AND category_id = $2 AND visibility_timestamp = $3 AND task_id = $4`
	rangeDeleteHistoryScheduledTasksQuery = `DELETE FROM history_scheduled_tasks WHERE shard_id = $1 AND category_id = $2 AND visibility_timestamp >= $3 AND visibility_timestamp < $4`

	createTransferTasksQuery = `INSERT INTO transfer_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getTransferTasksQuery = `SELECT task_id, data, data_encoding 
 FROM transfer_tasks WHERE shard_id = $1 AND task_id >= $2 AND task_id < $3 ORDER BY task_id LIMIT $4`

	deleteTransferTaskQuery      = `DELETE FROM transfer_tasks WHERE shard_id = $1 AND task_id = $2`
	rangeDeleteTransferTaskQuery = `DELETE FROM transfer_tasks WHERE shard_id = $1 AND task_id >= $2 AND task_id < $3`

	createTimerTasksQuery = `INSERT INTO timer_tasks (shard_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getTimerTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM timer_tasks 
  WHERE shard_id = $1 
  AND ((visibility_timestamp >= $2 AND task_id >= $3) OR visibility_timestamp > $4) 
  AND visibility_timestamp < $5
  ORDER BY visibility_timestamp,task_id LIMIT $6`

	deleteTimerTaskQuery      = `DELETE FROM timer_tasks WHERE shard_id = $1 AND visibility_timestamp = $2 AND task_id = $3`
	rangeDeleteTimerTaskQuery = `DELETE FROM timer_tasks WHERE shard_id = $1 AND visibility_timestamp >= $2 AND visibility_timestamp < $3`

	createReplicationTasksQuery = `INSERT INTO replication_tasks (shard_id, task_id, data, data_encoding) 
  VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getReplicationTasksQuery = `SELECT task_id, data, data_encoding FROM replication_tasks WHERE 
shard_id = $1 AND task_id >= $2 AND task_id < $3 ORDER BY task_id LIMIT $4`

	deleteReplicationTaskQuery      = `DELETE FROM replication_tasks WHERE shard_id = $1 AND task_id = $2`
	rangeDeleteReplicationTaskQuery = `DELETE FROM replication_tasks WHERE shard_id = $1 AND task_id >= $2 AND task_id < $3`

	getReplicationTasksDLQQuery = `SELECT task_id, data, data_encoding FROM replication_tasks_dlq WHERE 
source_cluster_name = $1 AND
shard_id = $2 AND
task_id >= $3 AND
task_id < $4
ORDER BY task_id LIMIT $5`

	createVisibilityTasksQuery = `INSERT INTO visibility_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getVisibilityTasksQuery = `SELECT task_id, data, data_encoding 
 FROM visibility_tasks WHERE shard_id = $1 AND task_id >= $2 AND task_id < $3 ORDER BY task_id LIMIT $4`

	deleteVisibilityTaskQuery      = `DELETE FROM visibility_tasks WHERE shard_id = $1 AND task_id = $2`
	rangeDeleteVisibilityTaskQuery = `DELETE FROM visibility_tasks WHERE shard_id = $1 AND task_id >= $2 AND task_id < $3`

	bufferedEventsColumns     = `shard_id, namespace_id, workflow_id, run_id, data, data_encoding`
	createBufferedEventsQuery = `INSERT INTO buffered_events(` + bufferedEventsColumns + `)
VALUES (:shard_id, :namespace_id, :workflow_id, :run_id, :data, :data_encoding)`

	deleteBufferedEventsQuery = `DELETE FROM buffered_events WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4`
	getBufferedEventsQuery    = `SELECT data, data_encoding FROM buffered_events WHERE shard_id = $1 AND namespace_id = $2 AND workflow_id = $3 AND run_id = $4 ORDER BY id`

	insertReplicationTaskDLQQuery = `
INSERT INTO replication_tasks_dlq 
            (source_cluster_name, 
             shard_id, 
             task_id, 
             data, 
             data_encoding) 
VALUES     (:source_cluster_name, 
            :shard_id, 
            :task_id, 
            :data, 
            :data_encoding)
`
	deleteReplicationTaskFromDLQQuery = `
	DELETE FROM replication_tasks_dlq 
		WHERE source_cluster_name = $1 
		AND shard_id = $2 
		AND task_id = $3`

	rangeDeleteReplicationTaskFromDLQQuery = `
	DELETE FROM replication_tasks_dlq 
		WHERE source_cluster_name = $1 
		AND shard_id = $2 
		AND task_id >= $3
		AND task_id < $4`
)

// InsertIntoExecutions inserts a row into executions table
func (pdb *db) InsertIntoExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createExecutionQuery,
		row,
	)
}

// UpdateExecutions updates a single row in executions table
func (pdb *db) UpdateExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		updateExecutionQuery,
		row,
	)
}

// SelectFromExecutions reads a single row from executions table
func (pdb *db) SelectFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (*sqlplugin.ExecutionsRow, error) {
	var row sqlplugin.ExecutionsRow
	err := pdb.GetContext(ctx,
		&row,
		getExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// DeleteFromExecutions deletes a single row from executions table
func (pdb *db) DeleteFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// ReadLockExecutions acquires a write lock on a single row in executions table
func (pdb *db) ReadLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, int64, error) {
	var executionVersion sqlplugin.ExecutionVersion
	err := pdb.GetContext(ctx,
		&executionVersion,
		readLockExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	return executionVersion.DBRecordVersion, executionVersion.NextEventID, err
}

// WriteLockExecutions acquires a write lock on a single row in executions table
func (pdb *db) WriteLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, int64, error) {
	var executionVersion sqlplugin.ExecutionVersion
	err := pdb.GetContext(ctx,
		&executionVersion,
		writeLockExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	return executionVersion.DBRecordVersion, executionVersion.NextEventID, err
}

// InsertIntoCurrentExecutions inserts a single row into current_executions table
func (pdb *db) InsertIntoCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createCurrentExecutionQuery,
		row,
	)
}

// UpdateCurrentExecutions updates a single row in current_executions table
func (pdb *db) UpdateCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		updateCurrentExecutionsQuery,
		row,
	)
}

// SelectFromCurrentExecutions reads one or more rows from current_executions table
func (pdb *db) SelectFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := pdb.GetContext(ctx,
		&row,
		getCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return &row, err
}

// DeleteFromCurrentExecutions deletes a single row in current_executions table
func (pdb *db) DeleteFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// LockCurrentExecutions acquires a write lock on a single row in current_executions table
func (pdb *db) LockCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := pdb.GetContext(ctx,
		&row,
		lockCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return &row, err
}

// LockCurrentExecutionsJoinExecutions joins a row in current_executions with executions table and acquires a
// write lock on the result
func (pdb *db) LockCurrentExecutionsJoinExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) ([]sqlplugin.CurrentExecutionsRow, error) {
	var rows []sqlplugin.CurrentExecutionsRow
	err := pdb.SelectContext(ctx,
		&rows,
		lockCurrentExecutionJoinExecutionsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return rows, err
}

// InsertIntoHistoryImmediateTasks inserts one or more rows into history_immediate_tasks table
func (pdb *db) InsertIntoHistoryImmediateTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryImmediateTasksRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createHistoryImmediateTasksQuery,
		rows,
	)
}

// RangeSelectFromHistoryImmediateTasks reads one or more rows from transfer_tasks table
func (pdb *db) RangeSelectFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksRangeFilter,
) ([]sqlplugin.HistoryImmediateTasksRow, error) {
	var rows []sqlplugin.HistoryImmediateTasksRow
	if err := pdb.SelectContext(ctx,
		&rows,
		getHistoryImmediateTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromHistoryImmediateTasks deletes one or more rows from transfer_tasks table
func (pdb *db) DeleteFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteHistoryImmediateTaskQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.TaskID,
	)
}

// RangeDeleteFromHistoryImmediateTasks deletes one or more rows from transfer_tasks table
func (pdb *db) RangeDeleteFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksRangeFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		rangeDeleteHistoryImmediateTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoHistoryScheduledTasks inserts one or more rows into timer_tasks table
func (pdb *db) InsertIntoHistoryScheduledTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryScheduledTasksRow,
) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(rows[i].VisibilityTimestamp)
	}
	return pdb.NamedExecContext(
		ctx,
		createHistoryScheduledTasksQuery,
		rows,
	)
}

// RangeSelectFromHistoryScheduledTasks reads one or more rows from timer_tasks table
func (pdb *db) RangeSelectFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) ([]sqlplugin.HistoryScheduledTasksRow, error) {
	var rows []sqlplugin.HistoryScheduledTasksRow
	filter.InclusiveMinVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	if err := pdb.SelectContext(ctx,
		&rows,
		getHistoryScheduledTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.InclusiveMinTaskID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromHistoryScheduledTasks deletes one or more rows from timer_tasks table
func (pdb *db) DeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksFilter,
) (sql.Result, error) {
	filter.VisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.VisibilityTimestamp)
	return pdb.ExecContext(ctx,
		deleteHistoryScheduledTaskQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.VisibilityTimestamp,
		filter.TaskID,
	)
}

// RangeDeleteFromHistoryScheduledTasks deletes one or more rows from timer_tasks table
func (pdb *db) RangeDeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) (sql.Result, error) {
	filter.InclusiveMinVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	return pdb.ExecContext(ctx,
		rangeDeleteHistoryScheduledTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
	)
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (pdb *db) InsertIntoTransferTasks(
	ctx context.Context,
	rows []sqlplugin.TransferTasksRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createTransferTasksQuery,
		rows,
	)
}

// RangeSelectFromTransferTasks reads one or more rows from transfer_tasks table
func (pdb *db) RangeSelectFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) ([]sqlplugin.TransferTasksRow, error) {
	var rows []sqlplugin.TransferTasksRow
	err := pdb.SelectContext(ctx,
		&rows,
		getTransferTasksQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (pdb *db) DeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteTransferTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (pdb *db) RangeDeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		rangeDeleteTransferTaskQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoTimerTasks inserts one or more rows into timer_tasks table
func (pdb *db) InsertIntoTimerTasks(
	ctx context.Context,
	rows []sqlplugin.TimerTasksRow,
) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(rows[i].VisibilityTimestamp)
	}
	return pdb.NamedExecContext(ctx,
		createTimerTasksQuery,
		rows,
	)
}

// RangeSelectFromTimerTasks reads one or more rows from timer_tasks table
func (pdb *db) RangeSelectFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) ([]sqlplugin.TimerTasksRow, error) {
	var rows []sqlplugin.TimerTasksRow
	filter.InclusiveMinVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	err := pdb.SelectContext(ctx,
		&rows,
		getTimerTasksQuery,
		filter.ShardID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.InclusiveMinTaskID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
		filter.PageSize,
	)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = pdb.converter.FromPostgreSQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (pdb *db) DeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksFilter,
) (sql.Result, error) {
	filter.VisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.VisibilityTimestamp)
	return pdb.ExecContext(ctx,
		deleteTimerTaskQuery,
		filter.ShardID,
		filter.VisibilityTimestamp,
		filter.TaskID,
	)
}

// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (pdb *db) RangeDeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) (sql.Result, error) {
	filter.InclusiveMinVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = pdb.converter.ToPostgreSQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	return pdb.ExecContext(ctx,
		rangeDeleteTimerTaskQuery,
		filter.ShardID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
	)
}

// InsertIntoBufferedEvents inserts one or more rows into buffered_events table
func (pdb *db) InsertIntoBufferedEvents(
	ctx context.Context,
	rows []sqlplugin.BufferedEventsRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createBufferedEventsQuery,
		rows,
	)
}

// SelectFromBufferedEvents reads one or more rows from buffered_events table
func (pdb *db) SelectFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) ([]sqlplugin.BufferedEventsRow, error) {
	var rows []sqlplugin.BufferedEventsRow
	if err := pdb.SelectContext(ctx,
		&rows,
		getBufferedEventsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
		rows[i].ShardID = filter.ShardID
	}
	return rows, nil
}

// DeleteFromBufferedEvents deletes one or more rows from buffered_events table
func (pdb *db) DeleteFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteBufferedEventsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// InsertIntoReplicationTasks inserts one or more rows into replication_tasks table
func (pdb *db) InsertIntoReplicationTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationTasksRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createReplicationTasksQuery,
		rows,
	)
}

// RangeSelectFromReplicationTasks reads one or more rows from replication_tasks table
func (pdb *db) RangeSelectFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) ([]sqlplugin.ReplicationTasksRow, error) {
	var rows []sqlplugin.ReplicationTasksRow
	err := pdb.SelectContext(ctx,
		&rows,
		getReplicationTasksQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteFromReplicationTasks deletes one rows from replication_tasks table
func (pdb *db) DeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteReplicationTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromReplicationTasks deletes multi rows from replication_tasks table
func (pdb *db) RangeDeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		rangeDeleteReplicationTaskQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoReplicationDLQTasks inserts one or more rows into replication_tasks_dlq table
func (pdb *db) InsertIntoReplicationDLQTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationDLQTasksRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		insertReplicationTaskDLQQuery,
		rows,
	)
}

// RangeSelectFromReplicationDLQTasks reads one or more rows from replication_tasks_dlq table
func (pdb *db) RangeSelectFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) ([]sqlplugin.ReplicationDLQTasksRow, error) {
	var rows []sqlplugin.ReplicationDLQTasksRow
	err := pdb.SelectContext(ctx,
		&rows, getReplicationTasksDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteFromReplicationDLQTasks deletes one row from replication_tasks_dlq table
func (pdb *db) DeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksFilter,
) (sql.Result, error) {

	return pdb.ExecContext(ctx,
		deleteReplicationTaskFromDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromReplicationDLQTasks deletes one or more rows from replication_tasks_dlq table
func (pdb *db) RangeDeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) (sql.Result, error) {

	return pdb.ExecContext(ctx,
		rangeDeleteReplicationTaskFromDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoVisibilityTasks inserts one or more rows into visibility_tasks table
func (pdb *db) InsertIntoVisibilityTasks(
	ctx context.Context,
	rows []sqlplugin.VisibilityTasksRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createVisibilityTasksQuery,
		rows,
	)
}

// RangeSelectFromVisibilityTasks reads one or more rows from visibility_tasks table
func (pdb *db) RangeSelectFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksRangeFilter,
) ([]sqlplugin.VisibilityTasksRow, error) {
	var rows []sqlplugin.VisibilityTasksRow
	err := pdb.SelectContext(ctx,
		&rows,
		getVisibilityTasksQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table
func (pdb *db) DeleteFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteVisibilityTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table
func (pdb *db) RangeDeleteFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksRangeFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		rangeDeleteVisibilityTaskQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}
