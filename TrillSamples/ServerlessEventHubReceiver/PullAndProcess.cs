using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.StreamProcessing;
using Newtonsoft.Json;

namespace ServerlessEventHubReceiver
{
    public static class PullAndProcess
    {
        [JsonObject(MemberSerialization.OptOut)]
        public struct Result
        {
            public QueryState QueryState;

            public bool IsCompleted;

            public int NumberEventsProcessed;
        }

        // An activity function that processes a batch of input events, produces a batch of output events, and updates the query state.
        // It is called periodically by the QueryController orchestration.
        [FunctionName(nameof(PullAndProcess))]
        public static async Task<Result> Run([ActivityTrigger] IDurableActivityContext context, ILogger logger)
        {
            QueryState queryState = context.GetInput<QueryState>();

            // create query
            Config.ForceRowBasedExecution = true;
            var queryContainer = new QueryContainer();
            var subject = new Subject<StreamEvent<long>>();
            var inputStream = queryContainer.RegisterInput(
                subject,
                DisorderPolicy.Drop(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(1));
            var query = inputStream.AlterEventDuration(StreamEvent.InfinitySyncTime).Count();

            // connect query output to a list
            var queryTask = queryContainer
                .RegisterOutput(query)
                .Where(e => e.IsStart)
                .ForEachAsync(e => Console.WriteLine($"Produced: {e}"));

            // restore query state
            Process queryProcess = (queryState.QueryCheckpoint == null)
                ? queryContainer.Restore()
                : queryContainer.Restore(new MemoryStream(queryState.QueryCheckpoint));

            // connect to EventHubs
            var connection = EventHubsConnection.Instance;

            // fetch and process events
            // (this modifies input.QueuePositions and calls OnNext on subject)
            var numberEventsProcessed = await connection.PullEventsAsync(queryState.QueuePositions, subject, queryTask, WaitPeriods.ActiveWait, CancellationToken.None);

            // checkpoint the query state
            var stream = new MemoryStream();
            queryProcess.Checkpoint(stream);

            queryState.QueryCheckpoint = stream.ToArray();

            // return the output
            return new Result()
            {
                QueryState = queryState,
                NumberEventsProcessed = numberEventsProcessed,
                IsCompleted = queryTask.IsCompleted,
            };
        }
    }
}
