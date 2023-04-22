using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Assigment1FetchDataFromAPI.Models;
using Azure.Data.Tables;
using System.Reflection.Metadata;

namespace Assigment1FetchDataFromAPI
{
    public static class DataAPI
    {
        // From date and to date should be provided as query parameter dateFrom and dateFrom.
        // Example: http://localhost:7039/api/GetFeatchAttemptLog?dateFrom=20.04.2023%2013:30&dateTo=20.04.2023%2016:30
        // Note: Table input binding is not working correctly when using Filter.
        // It is try to bind Table instead of IEnumerable<TableEntry>!!!
        // So, we filter data inside the function.
        [FunctionName(nameof(GetFeatchAttemptLog))]
        public static IActionResult GetFeatchAttemptLog(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            [Table("FeatchAttemptLogs", "api.publicapis.org", Connection = "StorageConnectionAppSetting")] TableClient featchAttemptLogs,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger GetFeatchAttemptLog function processed a request.");
            // We assume that 'dateFrom' and 'dateTo' queries are provided and in correct string format.
            Azure.Pageable<FeatchAttemptLog> queriedLogs;
            try
            {
                DateTime dateFrom = DateTime.Parse(req.Query["dateFrom"]);
                DateTime dateTo = DateTime.Parse(req.Query["dateTo"]);

                queriedLogs = featchAttemptLogs.Query<FeatchAttemptLog>(x => x.requestDateTime >= dateFrom && x.requestDateTime <= dateTo);
            }
            catch (Exception ex)
            {
                log.LogInformation("Problem during getting data from the table:");
                log.LogInformation(ex.ToString());
                return new BadRequestObjectResult("");

            }
            return new OkObjectResult(queriedLogs);
        }

       // We assume that the blob is requests by FetchAttempLog composite key PartitionKey and RowKey and
       // route is GetBlobForFeatchAttemptLog/{PartitionKey}/{RowKey}.
       // Note: No need to query the FetchAttempLog since we store blob in {PartitionKey}/{RowKey}.json.
       //       If location is different and stored in the record we must first query the FetchAttempLog.
       [FunctionName(nameof(GetBlobForFeatchAttemptLog))]
       public static async Task<IActionResult> GetBlobForFeatchAttemptLog(
       [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "GetBlobForFeatchAttemptLog/{PartitionKey}/{RowKey}")] HttpRequest req,
       string RowKey,
       [Blob("{PartitionKey}/{RowKey}.json", FileAccess.Read, Connection = "BlobStorageConnection")] Stream blobFileStream,
       ILogger log)
       {
            log.LogInformation("C# HTTP trigger GetBlobForFeatchAttemptLog function processed a request.");
            if (blobFileStream == null)
            {
                return new NotFoundObjectResult("");
            }

            // Copy to memory stream and return it as FileStreamResult.
            MemoryStream fileContent = new MemoryStream();
            await blobFileStream.CopyToAsync(fileContent);
            fileContent.Seek(0, SeekOrigin.Begin);

            return new FileStreamResult(fileContent, "application/octet-stream") {
                FileDownloadName = RowKey + ".json"
            };
        }
    }
}
