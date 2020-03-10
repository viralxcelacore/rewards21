using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;

namespace functions.CheckWhetherBlobExists
{
    public static class BlobExists
    {
        [FunctionName("BlobExists")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string blobUrl = req.Query["blobUrl"];
            HttpResponseMessage response = null;
            using (HttpClient httpClient = new HttpClient())
            {
                 response = await httpClient.GetAsync(blobUrl);
            }

            return response.StatusCode == System.Net.HttpStatusCode.NotFound
                ? (ActionResult)new OkObjectResult("No")
                : (ActionResult)new OkObjectResult("Yes");
        }
    }
}
