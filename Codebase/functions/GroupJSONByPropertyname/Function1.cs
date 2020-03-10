using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Linq;
using System;

namespace GroupByJsonByColumn
{
    public static class GroupJsonByColumn
    {
        [FunctionName("GroupJSONByPropertyName")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // parse query parameter
            string propertyName = req.Query["propertyName"];

            string requestBody = new StreamReader(req.Body).ReadToEnd();
            // Get request body
            JObject data = JObject.Parse(requestBody);

            var groupedRowData = from row in data["rows"].Values<JToken>()
                                 group row by row[propertyName]
                                 into g
                                 select new { groupedProperty = g.Key, groupedList = g.ToList() };


            return (ActionResult)new OkObjectResult(groupedRowData);
        }
    }
}
