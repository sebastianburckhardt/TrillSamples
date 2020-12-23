// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.StreamProcessing;

namespace EventHubSender
{
    public sealed class Program
    {
        private static string EventHubConnectionString = Environment.GetEnvironmentVariable("EventHubsConnection");
        private static string EventHubName = "trillsample";

        private static EventHubClient eventHubClient;

        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub();

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }

        // Creates an Event Hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub()
        {
            var proc = System.Diagnostics.Process.GetCurrentProcess();

            int messageCount = 0;
            long clock = DateTime.UtcNow.Ticks;

            while (true)
            {
                try
                {
                    clock = Math.Max(clock + 1, DateTime.UtcNow.Ticks);
                    var message = StreamEvent.CreateStart(clock, proc.WorkingSet64);
                    Console.WriteLine($"Sending message #{++messageCount}: {message}");
                    await eventHubClient.SendAsync(new EventData(BinarySerializer.Serialize(message)), "default");
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }
                // await Task.Delay(1000);
            }
        }
    }
}
