//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Table
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB.Table;
    using Microsoft.Azure.Storage;

    internal sealed class BasicWorker
    {
        private readonly IList<CloudTableClient> tableClients;
        private readonly string tableName;

        public BasicWorker(string tableName)
        {
            this.tableClients = new List<CloudTableClient>();
            this.tableName = tableName;
        }

        public void AddClient(CloudTableClient client)
        {
            this.tableClients.Add(client);
        }

        public Task InitializeAsync()
        {
            CloudTableClient createClient = this.tableClients[0];
            CloudTable cloudTable = createClient.GetTableReference(this.tableName);
            Console.WriteLine("\tInitializing Basic Table: {0} with CloudTableClient at Region: {1}", this.tableName, createClient.ConnectionPolicy.PreferredLocations[0]);
            return cloudTable.CreateIfNotExistsAsync();
        }

        public Task CleanUpAsync()
        {
            CloudTableClient deleteClient = this.tableClients[0];
            CloudTable cloudTable = deleteClient.GetTableReference(this.tableName);
            return cloudTable.DeleteIfExistsAsync();
        }

        public async Task RunBasicAsync(int entitiesNum)
        {
            Console.WriteLine("1) Starting insert loops across multiple regions ...");
            IList<Task> basicTasks = new List<Task>();

            string partitionKey = Guid.NewGuid().ToString();
            foreach (CloudTableClient client in this.tableClients)
            {
                basicTasks.Add(this.RunInsertAsync(entitiesNum, partitionKey, client));
            }

            await Task.WhenAll(basicTasks);
            basicTasks.Clear();

            Console.WriteLine("2) Reading from every region ...");
            int expectedEntities = this.tableClients.Count * entitiesNum;
            foreach (CloudTableClient client in this.tableClients)
            {
                basicTasks.Add(this.ReadAllAsync(expectedEntities, partitionKey, client));
            }

            await Task.WhenAll(basicTasks);
            basicTasks.Clear();

            Console.WriteLine("3) Deleting all the documents ...");
            IEnumerator<CloudTableClient> tableClientsEnumerator = this.tableClients.GetEnumerator();
            tableClientsEnumerator.MoveNext();
            await this.DeleteAllAsync(expectedEntities, partitionKey, tableClientsEnumerator.Current);
            while (tableClientsEnumerator.MoveNext())
            {
                basicTasks.Add(this.ReadAllAsync(0, partitionKey, tableClientsEnumerator.Current));
            }

            await Task.WhenAll(basicTasks);
            basicTasks.Clear();
        }

        private async Task RunInsertAsync(int entitiesToInsert, string partitionKey, CloudTableClient client)
        {
            CloudTable table = client.GetTableReference(this.tableName);
            int iterationCount = 0;

            List<long> latency = new List<long>();
            while (iterationCount++ < entitiesToInsert)
            {
                long startTick = Environment.TickCount;
                DynamicTableEntity entity = Helpers.GenerateRandomEntity(partitionKey);
                await table.ExecuteAsync(TableOperation.Insert(entity));

                long endTick = Environment.TickCount;

                latency.Add(endTick - startTick);
            }

            latency.Sort();
            int p50Index = latency.Count / 2;

            Console.WriteLine(
                "\tInserted {0} entities at {1} with p50 {2} ms",
                entitiesToInsert,
                client.ConnectionPolicy.PreferredLocations[0],
                latency[p50Index]);
        }

        private async Task<IList<DynamicTableEntity>> ReadAllAsync(int expectedNumberOfEntities, string partitionKey, CloudTableClient client)
        {
            CloudTable table = client.GetTableReference(this.tableName);
            TableQuery query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            List<DynamicTableEntity> results = new List<DynamicTableEntity>();
            int attempt = 0;

            do
            {
                attempt += 1;
                Console.WriteLine("\t#{0} attempt reading... Total {1} attempts", attempt, Helpers.RetryThreshold);
                await Task.Delay(Helpers.DelayTime);
                results.Clear();
                TableContinuationToken token = null;
                do
                {
                    TableQuerySegment<DynamicTableEntity> querySegment = await table.ExecuteQuerySegmentedAsync(query, token);
                    token = querySegment.ContinuationToken;
                    results.AddRange(querySegment.Results);
                }
                while (token != null);

                Console.WriteLine(
                    "\tRead {0} items from {1}, expected: {2}",
                    results.Count,
                    client.ConnectionPolicy.PreferredLocations[0],
                    expectedNumberOfEntities);
                if (results.Count == expectedNumberOfEntities)
                {
                    return results;
                }
            }
            while (attempt < Helpers.RetryThreshold);
            return results;
        }

        private async Task DeleteAllAsync(int expectedNumberOfEntities, string partitionKey, CloudTableClient client)
        {
            CloudTable table = client.GetTableReference(this.tableName);
            IList<DynamicTableEntity> entities = await this.ReadAllAsync(expectedNumberOfEntities, partitionKey, client);
            Console.WriteLine("\tDeleting entites from Region: {0}...", client.ConnectionPolicy.PreferredLocations[0]);

            foreach (DynamicTableEntity entity in entities)
            {
                try
                {
                    await table.ExecuteAsync(TableOperation.Delete(entity));
                }
                catch (StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode != (int)System.Net.HttpStatusCode.NotFound)
                    {
                        Console.WriteLine("\tError occurred while deleting {0} from {1}", exception, client.ConnectionPolicy.PreferredLocations[0]);
                    }
                }
            }

            Console.WriteLine("\tDeleted all documents from region {0}", client.ConnectionPolicy.PreferredLocations[0]);
        }
    }
}
