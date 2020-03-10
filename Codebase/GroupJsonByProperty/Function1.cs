using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GroupByJsonByColumn
{
    public static class GroupJsonByColumn
    {
        [FunctionName("GroupJSONByColumn")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // parse query parameter
            string propertyName = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "propertyName", true) == 0)
                .Value;

            // Get request body
            JObject data = JObject.Parse((await req.Content.ReadAsStringAsync()));

            var groupedRowData = from row in data["rows"].Values<JToken>()
                                 group row by row[propertyName]
                                 into g
                                 select new { groupedProperty = g.Key, groupedList = g.ToList() };


            return req.CreateResponse(HttpStatusCode.OK, groupedRowData);
        }
    }
}
