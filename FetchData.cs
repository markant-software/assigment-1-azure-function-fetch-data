using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml;
using Assigment1FetchDataFromAPI.Models;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using static System.Net.Mime.MediaTypeNames;

namespace Assigment1FetchDataFromAPI
{
    public class FetchData
    {
        private static readonly string DataProvider = System.Environment.GetEnvironmentVariable("DataProvider");
        private static readonly string DataProviderUrl = System.Environment.GetEnvironmentVariable("DataProviderUrl");
        private static readonly int FetchDataTimeOutSec = Convert.ToInt32(System.Environment.GetEnvironmentVariable("FetchDataTimeOutSec"));
        private static readonly string TimeFormat = "yyyyMMddHHmmss";


        [FunctionName(nameof(FeatchDataScheduler))]
        public static void FeatchDataScheduler([TimerTrigger("0 */1 * * * *")]TimerInfo myTimer,
              [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // Just start Orcestrator with UTC requested time.
            starter.StartNewAsync<DateTime>(nameof(FetchDataManager), null, DateTime.UtcNow);
        }


        [FunctionName(nameof(FetchDataManager))]
        public static async Task FetchDataManager(
           [OrchestrationTrigger] IDurableOrchestrationContext context,
           ILogger log)
        {
            // Fetch the data from URL.
            FetchedData fetchedData = await context.CallActivityAsync<FetchedData>(nameof(FetchDataFromApi), null);
            fetchedData.requestTime = context.GetInput<DateTime>();
            // Create rowKey based on the requested time
            fetchedData.rowKey = fetchedData.requestTime.ToString(TimeFormat);
            // If successful first store the blob.
            // Note: first we stroe the blob inorder to avoid get request when log is successful but no blob.
            if (fetchedData.success)
            {
                await context.CallActivityAsync(nameof(StoreBlob), fetchedData);
            }
            // Store the attempt log.
            await context.CallActivityAsync(nameof(StoreAttemptLog), fetchedData);
        }

        [FunctionName(nameof(FetchDataFromApi))]
        public static async Task<FetchedData> FetchDataFromApi(
          [ActivityTrigger] IDurableActivityContext context,
          ILogger log)
        {
            FetchedData fetchedData = new FetchedData();
            HttpClient httpClient = new HttpClient();
            log.LogInformation($"C# FetchDataFromApi: {DateTime.Now}");
            httpClient.Timeout = TimeSpan.FromSeconds(FetchDataTimeOutSec);
            HttpResponseMessage responseMessage = await httpClient.GetAsync(DataProviderUrl);
            if (responseMessage != null && responseMessage.IsSuccessStatusCode )
            {
                fetchedData.success = true;
                fetchedData.content = await responseMessage.Content.ReadAsStringAsync();

            }
            else
            {
                fetchedData.success = false;
                fetchedData.content = null;
            }
            
            return fetchedData;
        }

        // We store the blob on location [PartitionKey]\[RowKey].
        [FunctionName(nameof(StoreBlob))]
        public static async Task StoreBlob(
           [ActivityTrigger] FetchedData fetchedData,
           [Blob("api-publicapis-org/{fetchedData.rowKey}.json", FileAccess.Write, Connection = "BlobStorageConnection")] Stream blobFileStream,
           ILogger log)
        {
            log.LogInformation($"C# StoreBlob: {DateTime.Now}");

            // Write the content in to a blob file.
            var writer = new StreamWriter(blobFileStream);
            await writer.WriteAsync(fetchedData.content);
            await writer.FlushAsync();
        }

        [FunctionName(nameof(StoreAttemptLog))]
        public static void StoreAttemptLog(
            [ActivityTrigger] FetchedData fetchedData,
            [Table("FeatchAttemptLogs", Connection = "StorageConnectionAppSetting")] out FeatchAttemptLog featchAttemptLog,
            ILogger log
            )
        {
            // Note: We store the blob on location [PartitionKey]\[RowKey] so no need to keep file location in Table.
            //       If location is different than we should store it Table and use for query blob.
            log.LogInformation($"C# StoreAttemptLog: {DateTime.Now}");
            featchAttemptLog = new FeatchAttemptLog()
            {
                PartitionKey = DataProvider,
                RowKey = fetchedData.rowKey,
                requestDateTime = fetchedData.requestTime,
                success = fetchedData.success
            };
        }
    }
}
