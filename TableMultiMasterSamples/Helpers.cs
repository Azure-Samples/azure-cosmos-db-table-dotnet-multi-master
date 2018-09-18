//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Table
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB;
    using Microsoft.Azure.CosmosDB.Table;
    using Microsoft.Azure.Storage;
    using Microsoft.Azure.Storage.Auth;

    internal static class Helpers
    {
        public static readonly int RetryThreshold = 5;
        public static readonly int DelayTime = 5000;
        public static readonly Random Random = new Random(1000);

        public static CloudTableClient GenerateCloudTableClient(string accountName, string accountEndpoint, string accountKey, string region)
        {
            Uri uri = new Uri(accountEndpoint);
            TableConnectionPolicy connectionPolicy = new TableConnectionPolicy();
            connectionPolicy.UseTcpProtocol = true;
            connectionPolicy.UseMultipleWriteLocations = true;
            connectionPolicy.PreferredLocations.Add(region);
            StorageCredentials credentials = new StorageCredentials(accountName, accountKey);
            StorageUri storageUri = new StorageUri(uri);
            return new CloudTableClient(storageUri, credentials, connectionPolicy, ConsistencyLevel.Eventual);
        }

        public static DynamicTableEntity GenerateRandomEntity(string pk)
        {
            DynamicTableEntity ent = new DynamicTableEntity()
            {
                PartitionKey = pk,
                RowKey = DateTime.Now.Ticks.ToString() + Random.Next(),
            };
            return ent;
        }

        public static void TraceError(string format, params object[] values)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(format, values);
            Console.ResetColor();
        }

        public static DynamicTableEntity Clone(DynamicTableEntity source)
        {
            DynamicTableEntity entity = new DynamicTableEntity(source.PartitionKey, source.RowKey);
            entity.Timestamp = source.Timestamp;
            entity.ETag = source.ETag;
            entity.Properties = source.Properties.ToDictionary(entry => entry.Key, entry => entry.Value);
            return entity;
        }
    }
}
