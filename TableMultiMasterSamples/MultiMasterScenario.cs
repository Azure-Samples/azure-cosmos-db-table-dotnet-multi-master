//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Table
{
    using System;
    using System.Configuration;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB.Table;

    internal sealed class MultiMasterScenario
    {
        private static readonly int EntityNumbers = 10;
        private readonly string accountName;
        private readonly string accountEndpoint;
        private readonly string accountKey;

        private BasicWorker basicWorker;
        private ConflictWorker conflictWorker;

        public MultiMasterScenario()
        {
            this.accountName = ConfigurationManager.AppSettings["account"];
            this.accountEndpoint = ConfigurationManager.AppSettings["endpoint"];
            this.accountKey = ConfigurationManager.AppSettings["key"];

            string[] regions = ConfigurationManager.AppSettings["regions"].Split(
                new string[] { ";" },
                StringSplitOptions.RemoveEmptyEntries);
            string tableName = ConfigurationManager.AppSettings["tableName"];

            string basicWorkerTableName = tableName + "_basic";
            this.basicWorker = new BasicWorker(basicWorkerTableName);
            string conflictWorkerTableName = tableName + "_conflict";
            this.conflictWorker = new ConflictWorker(conflictWorkerTableName);

            Console.WriteLine("\n####################################################");
            Console.Write(
                "Account: {0}; Endpoint: {1}; BasicWorker TableName: {2}; ConflictWorker TableName: {3}; ",
                this.accountName,
                this.accountEndpoint,
                basicWorkerTableName,
                conflictWorkerTableName);

            foreach (string region in regions)
            {
                CloudTableClient client =
                    Helpers.GenerateCloudTableClient(this.accountName, this.accountEndpoint, this.accountKey, region);
                Console.Write("Region: {0}; ", region);
                this.basicWorker.AddClient(client);
                this.conflictWorker.AddClient(client);
            }

            Console.Write("\n####################################################\n");
        }

        public async Task InitializeAsync()
        {
            Console.WriteLine("\n####################################################");
            await this.CleanUpAsync();
            await this.basicWorker.InitializeAsync();
            await this.conflictWorker.InitializeAsync();
            Console.WriteLine("\tInitialized tables.");
            Console.Write("####################################################\n");

            // Give some seconds to allow table creation get populated to all regions
            await Task.Delay(Helpers.DelayTime * 3);
        }

        public async Task RunBasicAsync()
        {
            Console.WriteLine("\n####################################################");
            Console.WriteLine("Basic Active-Active");
            Console.WriteLine("####################################################");
            await this.basicWorker.RunBasicAsync(MultiMasterScenario.EntityNumbers);
            Console.WriteLine("####################################################\n");
        }

        public async Task RunConflictAsync()
        {
            Console.WriteLine("\n####################################################");
            Console.WriteLine("Conflict Resolution");
            Console.WriteLine("####################################################");

            await this.conflictWorker.RunConflictAsync();
            Console.WriteLine("####################################################\n");
        }

        public async Task CleanUpAsync()
        {
            Console.WriteLine("\n####################################################");
            Console.WriteLine("\nCleaning up resource");
            await this.basicWorker.CleanUpAsync();
            await this.conflictWorker.CleanUpAsync();
            Console.WriteLine("Resource Cleaned up");
            Console.WriteLine("####################################################\n");
        }
    }
}
