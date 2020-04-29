select queryMetrics.queryId, runId, host, startTime,
      endTime, executionTimeMs, epoch, metricComponent, value  from queryMetrics left join
      cpuPercentSoftirq on queryMetrics.queryId = cpuPercentSoftirq.queryId where
      cpuPercentSoftirq.epochInMilliSeconds >= startTime and
      cpuPercentSoftirq.epochInMilliSeconds <= endTime