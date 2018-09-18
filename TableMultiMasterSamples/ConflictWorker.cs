//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Table
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB.Table;
    using Microsoft.Azure.Storage;

    internal sealed class ConflictWorker
    {
        private readonly IList<CloudTableClient> tableClients;
        private readonly string tableName;

        public ConflictWorker(string tableName)
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

        public async Task RunConflictAsync()
        {
            Console.WriteLine("\n1) Insert Conflict");
            await this.RunInsertConflictAsync();

            Console.WriteLine("\n2) Update Conflict");
            await this.RunUpdateConflictAsync();

            Console.WriteLine("\n3) Delete Conflict");
            await this.RunDeleteConflictAsync();
        }

        public async Task RunInsertConflictAsync()
        {
            DynamicTableEntity oneInsertedEntity = await this.PerformInsertConflictAsync();

            await this.ValidateConflictResolutionAsync(oneInsertedEntity);
        }

        public async Task RunUpdateConflictAsync()
        {
            DynamicTableEntity conflictEntity = await this.PerformUpdateConflictAsync();
            await this.ValidateConflictResolutionAsync(conflictEntity);
        }

        public async Task RunDeleteConflictAsync()
        {
            DynamicTableEntity conflictEntity = await this.PerformDeleteConflictAsync(true);

            // Delete should always win.
            await this.ValidateConflictResolutionAsync(conflictEntity, true);
        }

        public async Task<DynamicTableEntity> PerformInsertConflictAsync()
        {
            int attempt = 0;

            do
            {
                attempt += 1;
                Console.WriteLine("\t#{0} of total {1} attempt ...", attempt, Helpers.RetryThreshold);
                DynamicTableEntity oneInsertedEntity = Helpers.GenerateRandomEntity(Guid.NewGuid().ToString());

                Console.WriteLine(
                        "\t1) Performing conflicting insert across {0} regions.",
                        this.tableClients.Count);

                IList<Task<DynamicTableEntity>> insertTask = new List<Task<DynamicTableEntity>>();
                int index = 0;
                foreach (CloudTableClient client in this.tableClients)
                {
                    insertTask.Add(this.TryInsertTableEntityAsync(client, oneInsertedEntity, index++, true));
                }

                DynamicTableEntity[] conflicts = await Task.WhenAll(insertTask);

                int numberOfSuccessInsert = 0;
                foreach (DynamicTableEntity entity in conflicts)
                {
                    if (entity != null)
                    {
                        ++numberOfSuccessInsert;
                    }
                }

                if (numberOfSuccessInsert > 1)
                {
                    Console.WriteLine(
                        "\t2) Successfully inserted {0} entities with Partition Key: {1} and Row Key: {2}",
                        numberOfSuccessInsert,
                        oneInsertedEntity.PartitionKey,
                        oneInsertedEntity.RowKey);
                    return oneInsertedEntity;
                }
                else
                {
                    Console.WriteLine("\tOnly {0} entity inserted, retrying insert to induce conflicts", numberOfSuccessInsert);
                    await Task.Delay(Helpers.DelayTime);
                }
            }
            while (attempt < Helpers.RetryThreshold);
            return null;
        }

        public async Task<DynamicTableEntity> PerformUpdateConflictAsync()
        {
            int attempt = 0;
            DynamicTableEntity conflictEntity = null;
            do
            {
                attempt += 1;
                conflictEntity = Helpers.GenerateRandomEntity(Guid.NewGuid().ToString());
                Console.WriteLine("\t#{0} of total {1} attempt ...", attempt, Helpers.RetryThreshold);
                conflictEntity = await this.TryInsertTableEntityAsync(this.tableClients[0], conflictEntity, 0);
                await Task.Delay(Helpers.DelayTime);

                Console.WriteLine(
                    "\t1) Performing conflicting update across {0} regions.",
                    this.tableClients.Count);

                TableResult result = await this.tableClients[0].GetTableReference(this.tableName).ExecuteAsync(TableOperation.Retrieve(conflictEntity.PartitionKey, conflictEntity.RowKey));
                conflictEntity = result.Result as DynamicTableEntity;

                IList<Task<DynamicTableEntity>> updateTask = new List<Task<DynamicTableEntity>>();

                int index = 0;
                foreach (CloudTableClient client in this.tableClients)
                {
                    updateTask.Add(
                        this.TryUpdateEntityAsync(
                            client,
                            conflictEntity,
                            index++));
                }

                DynamicTableEntity[] conflicts = await Task.WhenAll(updateTask);

                int numberOfUpdates = 0;
                foreach (DynamicTableEntity entity in conflicts)
                {
                    if (entity != null)
                    {
                        ++numberOfUpdates;
                    }
                }

                if (numberOfUpdates > 1)
                {
                    Console.WriteLine(
                        "\t2) Successfully update {0} entities with Partition Key: {1} Row Key {2}, verifying conflict resolution",
                        numberOfUpdates,
                        conflictEntity.PartitionKey,
                        conflictEntity.RowKey);

                    return conflictEntity;
                }
                else
                {
                    Console.WriteLine("\tOnly {0} entity inserted, Retrying update to induce conflicts", numberOfUpdates);
                    await Task.Delay(Helpers.DelayTime);
                }
            }
            while (attempt < Helpers.RetryThreshold);

            return conflictEntity;
        }

        public async Task<DynamicTableEntity> PerformDeleteConflictAsync(bool bMixUpdateConflict)
        {
            DynamicTableEntity conflictEntity = null;
            int attempt = 0;
            do
            {
                attempt += 1;
                conflictEntity = Helpers.GenerateRandomEntity(Guid.NewGuid().ToString());
                Console.WriteLine("\t#{0} of total {1} attempt ...", attempt, Helpers.RetryThreshold);
                conflictEntity = await this.TryInsertTableEntityAsync(this.tableClients[0], conflictEntity, 0);

                await Task.Delay(Helpers.DelayTime);

                Console.WriteLine(
                    "\t1) Performing conflicting delete across {0} regions.",
                    this.tableClients.Count);

                TableResult result = await this.tableClients[0].GetTableReference(this.tableName).ExecuteAsync(TableOperation.Retrieve(conflictEntity.PartitionKey, conflictEntity.RowKey));
                conflictEntity = result.Result as DynamicTableEntity;

                IList<Task<DynamicTableEntity>> deleteTask = new List<Task<DynamicTableEntity>>();

                int index = 0;
                foreach (CloudTableClient client in this.tableClients)
                {
                    if (index % 2 == 1 || !bMixUpdateConflict)
                    {
                        deleteTask.Add(
                            this.TryDeleteEntityAsync(
                            client,
                            conflictEntity,
                            index++));
                    }
                    else
                    {
                        deleteTask.Add(
                            this.TryUpdateEntityAsync(
                                client,
                                conflictEntity,
                                index++));
                    }
                }

                DynamicTableEntity[] conflictEntities = await Task.WhenAll(deleteTask);

                int numberOfConflicts = 0;
                foreach (DynamicTableEntity entity in conflictEntities)
                {
                    if (entity != null)
                    {
                        ++numberOfConflicts;
                    }
                }

                if (numberOfConflicts > 1)
                {
                    Console.WriteLine("\t2) Caused {0} delete conflicts, verifying conflict resolution", numberOfConflicts);

                    return conflictEntity;
                }
                else
                {
                    Console.WriteLine("\tRetrying update/delete to induce conflicts");
                    await Task.Delay(Helpers.DelayTime);
                }
            }
            while (attempt < Helpers.RetryThreshold);

            return conflictEntity;
        }

        public async Task VerifyEntityNotExistsAsync(CloudTableClient client, DynamicTableEntity entity)
        {
            int attempt = 0;
            do
            {
                attempt += 1;
                try
                {
                    TableResult result = await client.GetTableReference(this.tableName)
                        .ExecuteAsync(TableOperation.Retrieve(entity.PartitionKey, entity.RowKey));
                    if (result.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        Console.WriteLine("\tDelete conflict won @ {0}", client.ConnectionPolicy.PreferredLocations[0]);
                        break;
                    }
                    else
                    {
                        Helpers.TraceError(
                            "\tDelete conflict for entity {0}-{1} didn't win @ {2}, retrying ...",
                            entity.PartitionKey,
                            entity.RowKey,
                            client.ConnectionPolicy.PreferredLocations[0]);

                        await Task.Delay(Helpers.DelayTime);
                    }
                }
                catch (StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    {
                        Console.WriteLine("\tDelete conflict won @ {0}", client.ConnectionPolicy.PreferredLocations[0]);
                        break;
                    }
                    else
                    {
                        Helpers.TraceError(
                            "\tDelete conflict for document {0}-{1} didn't win @ {2}",
                            entity.PartitionKey,
                            entity.RowKey,
                            client.ConnectionPolicy.PreferredLocations[0]);

                        await Task.Delay(Helpers.DelayTime);
                    }
                }
            }
            while (attempt < Helpers.RetryThreshold);
        }

        public async Task ValidateConflictResolutionAsync(DynamicTableEntity sampleEntity, bool hasDeleteConflict = false)
        {
            await Task.Delay(Helpers.DelayTime);
            if (hasDeleteConflict)
            {
                IList<Task> verifyTask = new List<Task>();
                foreach (CloudTableClient client in this.tableClients)
                {
                    verifyTask.Add(this.VerifyEntityNotExistsAsync(client, sampleEntity));
                }

                await Task.WhenAll(verifyTask);
                return;
            }

            bool conflictResolved = false;
            int attempt = 0;
            do
            {
                attempt += 1;
                await Task.Delay(Helpers.DelayTime);
                conflictResolved = await this.VerifyAllEntityMatchAsync(sampleEntity);
            }
            while (!conflictResolved && attempt < Helpers.RetryThreshold);

            if (!conflictResolved)
            {
                Console.WriteLine("\n\tFailed to wait for entity to converge in all regions.");
            }
        }

        private Task<bool> VerifyAllEntityMatchAsync(DynamicTableEntity sampleEntity)
        {
            bool conflictResolved = true;
            DynamicTableEntity flagEntity = null;
            foreach (CloudTableClient client in this.tableClients)
            {
                try
                {
                    TableResult result = client.GetTableReference(this.tableName).ExecuteAsync(TableOperation.Retrieve(sampleEntity.PartitionKey, sampleEntity.RowKey)).Result;
                    DynamicTableEntity retrivedEntity = result.Result as DynamicTableEntity;
                    if (flagEntity == null)
                    {
                        flagEntity = retrivedEntity;
                    }
                    else
                    {
                        if ((flagEntity.Properties.Count == retrivedEntity.Properties.Count) &&
                            flagEntity.Properties["regionId"].Equals(retrivedEntity.Properties["regionId"]) &&
                            flagEntity.Properties["region"].Equals(retrivedEntity.Properties["region"]) &&
                            flagEntity.ETag.Equals(retrivedEntity.ETag) &&
                            flagEntity.Timestamp.Equals(retrivedEntity.Timestamp))
                        {
                            conflictResolved = true;
                        }
                        else
                        {
                            conflictResolved = false;
                            break;
                        }
                    }
                }
                catch (Exception exception)
                {
                    Console.WriteLine("\tException happens during verifying if all entity is matching: {0} at region: {1}", exception.Message, client.ConnectionPolicy.PreferredLocations[0]);
                    conflictResolved = false;
                }
            }

            if (conflictResolved)
            {
                Console.WriteLine(
                    "\tAll entities retrieved from clients with different preferred location are matched. RegionId: {0}, Region: {1}, Etag: {2}, Timestamp: {3}.\n",
                    flagEntity.Properties["regionId"],
                    flagEntity.Properties["region"],
                    flagEntity.ETag,
                    flagEntity.Timestamp);
            }

            return Task.FromResult(conflictResolved);
        }

        private async Task<DynamicTableEntity> TryInsertTableEntityAsync(CloudTableClient client, DynamicTableEntity entity, int index, bool addProperty = false)
        {
            try
            {
                DynamicTableEntity clonedEntity = Helpers.Clone(entity);
                if (addProperty)
                {
                    clonedEntity.Properties["regionId"] = EntityProperty.GeneratePropertyForInt(index);
                    clonedEntity.Properties["region"] = EntityProperty.GeneratePropertyForString(client.ConnectionPolicy.PreferredLocations[0]);
                }

                Console.WriteLine("\tInserting entity with Partition Key :{0}, Row Key: {1} to region: {2}", entity.PartitionKey, entity.RowKey, client.ConnectionPolicy.PreferredLocations[0]);
                TableResult result = await client.GetTableReference(this.tableName).ExecuteAsync(TableOperation.Insert(clonedEntity));
                return entity;
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    return null;
                }

                throw;
            }
        }

        private async Task<DynamicTableEntity> TryUpdateEntityAsync(CloudTableClient client, DynamicTableEntity entity, int index)
        {
            try
            {
                DynamicTableEntity clonedEntity = Helpers.Clone(entity);
                clonedEntity.Properties["regionId"] = EntityProperty.GeneratePropertyForInt(index);
                clonedEntity.Properties["region"] = EntityProperty.GeneratePropertyForString(client.ConnectionPolicy.PreferredLocations[0]);

                Console.WriteLine("\tUpdating entity with Partition Key :{0}, Row Key: {1} to region: {2}", entity.PartitionKey, entity.RowKey, client.ConnectionPolicy.PreferredLocations[0]);
                TableResult result = await client.GetTableReference(this.tableName).ExecuteAsync(TableOperation.Replace(clonedEntity));
                return entity;
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed ||
                    exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw;
            }
        }

        private async Task<DynamicTableEntity> TryDeleteEntityAsync(CloudTableClient client, DynamicTableEntity entity, int index)
        {
            try
            {
                DynamicTableEntity clonedEntity = Helpers.Clone(entity);

                Console.WriteLine("\tDeleting entity with Partition Key :{0}, Row Key: {1} to region: {2}", entity.PartitionKey, entity.RowKey, client.ConnectionPolicy.PreferredLocations[0]);
                await client.GetTableReference(this.tableName).ExecuteAsync(TableOperation.Delete(clonedEntity));
                return entity;
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound ||
                    exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    return null;
                }

                throw;
            }
        }
    }
}
