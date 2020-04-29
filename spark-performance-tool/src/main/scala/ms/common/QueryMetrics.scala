package ms.common

import org.joda.time.DateTime

/**
 * Metrics logged per query performance run.
 *
 * @param queryId  queryId identifies each query uniquely.
 * @param runId indicates the runId of from the same queryId.
 * @param runTimes execution times of the query.
 * @param queryResultValidationSuccess if true the query output was expected.
 */
case class QueryMetrics(queryId: String,
                        runId: Int,
                        runTimes: RunTimes,
                        queryResultValidationSuccess: Boolean)

/**
 * Execution times.
 * @param startTime start time.
 * @param endTime   end time.
 * @param executionTimeMs execution time in milliseconds.
 */
case class RunTimes(startTime: Long,
                    endTime: Long,
                    executionTimeMs: Long)


case class QueryMetricsResults(query_type: String,
                               query_name: String,
                               run_id: Int,
                               start_time: String,
                               end_time: String,
                               queryResultValidationStatus: Boolean,
                               executaion_duration_millisec: Long,
                               executaion_duration_minutes: Long)

