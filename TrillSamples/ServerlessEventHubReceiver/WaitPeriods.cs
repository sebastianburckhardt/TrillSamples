using System;
using System.Collections.Generic;
using System.Text;

namespace ServerlessEventHubReceiver
{
    public static class WaitPeriods
    {
        /// <summary>
        /// How long to wait actively, after receiving the last event, until going into passive wait
        /// </summary>
        public static TimeSpan ActiveWait = TimeSpan.FromSeconds(5);

        /// <summary>
        /// How long to remain in passive wait until checking for more events
        /// </summary>
        public static TimeSpan PassiveWait = TimeSpan.FromSeconds(30);
    }
}
