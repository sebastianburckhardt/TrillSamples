using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace ServerlessEventHubReceiver
{
    /// <summary>
    /// Encapsulate the current state of a query, as carried forward between activities
    /// </summary>
    [JsonObject(MemberSerialization.OptOut)]
    public struct QueryState
    {
        /// <summary>
        /// A serialized checkpoint of the query state, or null if the query has not started
        /// </summary>
        public byte[] QueryCheckpoint;

        /// <summary>
        /// The positions within the EventHubs queue that have been processed
        /// </summary>
        public long[] QueuePositions;
    }
}
