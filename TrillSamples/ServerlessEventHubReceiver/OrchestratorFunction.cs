using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.StreamProcessing;
using Newtonsoft.Json;

namespace ServerlessEventHubReceiver
{
    public static class OrchestratorFunction
    {

        /// <summary>
        /// The orchestrator that periodically schedules the PullAndProcess function until the query is complete.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        [FunctionName(nameof(OrchestratorFunction))]
        public static async Task Run([OrchestrationTrigger] IDurableOrchestrationContext context, ILogger logger)
        {
            var queryState = context.GetInput<QueryState>();
            TimeSpan waitPeriod = TimeSpan.FromMinutes(1);

            // execute the PullAndProcess activity
            var result = await context.CallActivityAsync<PullAndProcess.Result>(nameof(PullAndProcess), queryState);

            Debug.Assert(result.NumberEventsProcessed == Enumerable
                    .Range(0, queryState.QueuePositions.Length)
                    .Select(i => result.QueryState.QueuePositions[i] - queryState.QueuePositions[i])
                    .Sum(), "number of queue positions advanced must match number of events processed");

            if (result.IsCompleted)
            {
                return;
            }
            else if (result.NumberEventsProcessed == 0)
            {
                if (!context.IsReplaying)
                {
                    logger.LogWarning("No new events in last {activeWait}. Going to sleep for {passiveWait}.", WaitPeriods.ActiveWait, WaitPeriods.PassiveWait);
                }

                await context.CreateTimer(context.CurrentUtcDateTime + WaitPeriods.PassiveWait, CancellationToken.None);
            }

            // start from the beginning, with updated query state
            context.ContinueAsNew(result.QueryState);
        }
    }
}