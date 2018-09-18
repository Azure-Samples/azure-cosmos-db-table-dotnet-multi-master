//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Table
{
    using System;
    using System.Text;
    using System.Threading.Tasks;

    public class Program
    {
        public static void Main(string[] args)
        {
            Program.RunScenariosAsync().GetAwaiter().GetResult();
        }

        private static async Task RunScenariosAsync()
        {
            MultiMasterScenario scenario = new MultiMasterScenario();

            try
            {
                await scenario.InitializeAsync();
                await scenario.RunBasicAsync();
                await scenario.RunConflictAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine("Meeting exception while running multi master table samples: \n{0}", e);
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Press any key to exit");
                Console.Read();
            }
        }
    }
}
