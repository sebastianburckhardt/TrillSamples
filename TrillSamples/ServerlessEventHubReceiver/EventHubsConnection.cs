using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.StreamProcessing;

namespace ServerlessEventHubReceiver
{
    /// <summary>
    /// Encapsulates the connection to EventHubs, and caches clients
    /// </summary>
    internal class EventHubsConnection
    {
        private static string EventHubConnectionString = Environment.GetEnvironmentVariable("EventHubsConnection");
        private static string EventHubName = "trillsample";


        private static EventHubsConnection instance;
        private static readonly object creationLock = new object();

        /// <summary>
        /// The minimum time for which we pull events from event hubs
        /// </summary>
        private TimeSpan PullWindowMin = TimeSpan.FromSeconds(20);

        /// <summary>
        /// The maximum time for which we pull events from event hubs
        /// </summary>
        private TimeSpan PullWindowMax = TimeSpan.FromSeconds(60);

        private readonly EventHubClient client;

        private EventHubsConnection(EventHubClient client)
        {
            this.client = client;
        }

        public static EventHubsConnection Instance
        {
            get
            {
                return instance ?? CreateUnderLock();

                EventHubsConnection CreateUnderLock()
                {
                    lock (creationLock)
                    {
                        if (instance == null)
                        {
                            // connect to EventHubs
                            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
                            {
                                EntityPath = EventHubName,
                            };

                            instance = new EventHubsConnection(EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString()));
                        }

                        return instance;
                    }
                }
            }
        }

        public enum StartPositionOptions
        {
            FromZero,
            FromCurrent,
        }

        public async Task<long[]> GetStartingPositions(StartPositionOptions startPosition)
        {
            var runtimeInformation = await this.client.GetRuntimeInformationAsync();
            int numberPartitions = runtimeInformation.PartitionCount;
            var result = new Dictionary<int, int>();

            var infoTasks = runtimeInformation.PartitionIds.Select(id => this.client.GetPartitionRuntimeInformationAsync(id)).ToList();
            await Task.WhenAll(infoTasks);

            var positions = new long[numberPartitions];

            if (startPosition == StartPositionOptions.FromCurrent)
            {
                for (int i = 0; i < numberPartitions; i++)
                {
                    var queueInfo = await infoTasks[i].ConfigureAwait(false);
                    positions[i] = queueInfo.LastEnqueuedSequenceNumber + 1;
                }
            }

            return positions;
        }

        public async Task<int> PullEventsAsync(
            long[] positions,
            IObserver<StreamEvent<long>> observer,
            Task queryTask,
            TimeSpan waitTime,
            CancellationToken token)
        {
            var starttime = DateTime.UtcNow;
            int numberEventsProcessed = 0;
            var channel = Channel.CreateBounded<StreamEvent<long>>(500);
            var idle = new bool[positions.Length];
            var pulltasks = new Task[positions.Length];

            // start the processing task
            var processTask = ProcessEventsAsync();

            // run a partition pull loop for each partition, on a separate thread pool task
            for (int i = 0; i < positions.Length; i++)
            {
                int ii = i;
                pulltasks[ii] = Task.Run(() => PullEventsAsync(ii));
            }

            // wait for the pull tasks to complete
            await Task.WhenAll(pulltasks);
            // tell the processing task no more events are coming
            channel.Writer.TryComplete();
            // wait for the processing task to complete
            await processTask;

            return numberEventsProcessed;

            async Task ProcessEventsAsync()
            {
                while (!token.IsCancellationRequested)
                {
                    if (!await channel.Reader.WaitToReadAsync(token).ConfigureAwait(false))
                    {
                        return;
                    }

                    if (channel.Reader.TryRead(out StreamEvent<long> streamEvent))
                    {
                        observer.OnNext(streamEvent);
                        numberEventsProcessed++;
                    }
                }
            }

            async Task PullEventsAsync(int partition)
            {
                var pos = EventPosition.FromSequenceNumber(positions[partition] - 1, inclusive: false);
                PartitionReceiver receiver = this.client.CreateReceiver("$Default", partition.ToString(), pos);
                while (!StopPulling())
                {
                    IEnumerable<EventData> eventDatas = await receiver.ReceiveAsync(200, waitTime);
                    if (eventDatas != null)
                    {
                        idle[partition] = false;
                        foreach (var eventData in eventDatas)
                        {
                            Console.WriteLine($"Received event {partition}.{eventData.SystemProperties.SequenceNumber}");
                            bool isSequenceCorrect = positions[partition]++ == eventData.SystemProperties.SequenceNumber;
                            Debug.Assert(isSequenceCorrect, "PartitionReceiver should deliver events in order");
                            var streamEvent = BinarySerializer.DeserializeStreamEventLong(eventData.Body.ToArray());
                            await channel.Writer.WriteAsync(streamEvent, token);
                        }
                    }
                    else
                    {
                        idle[partition] = true;
                    }
                }
            }

            bool StopPulling()
            {
                // we terminate with an exception if cancellation is requested
                token.ThrowIfCancellationRequested();

                // we stop pulling if the query has terminated
                if (queryTask.IsCompleted)
                {
                    return true;
                }

                // otherwise, we check if time is between min and max
                var elapsed = DateTime.UtcNow - starttime;

                if (elapsed > PullWindowMax)
                {
                    return true;
                }
                if (elapsed > PullWindowMin)
                {
                    return idle.All(b => b);
                }

                return false;
            }
        }
    }
}