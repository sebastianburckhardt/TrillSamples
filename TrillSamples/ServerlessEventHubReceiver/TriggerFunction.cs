using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace ServerlessEventHubReceiver
{
    public static class TriggerFunction
    {
        /// <summary>
        /// An Http trigger function that initiates the query in response to a POST request.
        /// The request body must contain the instance id, a string that identifies the query instance.
        /// </summary>
        [FunctionName(nameof(TriggerFunction))]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "start")] HttpRequest req,
            [DurableClient] IDurableClient client,
            ILogger log)
        {
            try
            {
                log.LogInformation("Starting query");

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string instanceId = requestBody;

                EventHubsConnection connection = EventHubsConnection.Instance;

                var queryState = new QueryState()
                {
                     QueryCheckpoint = null, // start a fresh query
                     QueuePositions = await connection.GetStartingPositions(EventHubsConnection.StartPositionOptions.FromZero),
                };

                await client.StartNewAsync<QueryState>(nameof(OrchestratorFunction), instanceId, queryState);

                return new AcceptedResult();
            }
            catch (Exception e)
            {
                return new BadRequestObjectResult(e);
            }
        }
    }
}
