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
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using RestSharp;
using System.Text.RegularExpressions;

namespace functions.GetFailedMoglLocalBlob
{
    public static class GetFailedMoglLocalBlob
    {
        [FunctionName("GetFailedMoglLocalBlob")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string invoice = req.Query["invoiceId"];
            char[] fieldSeperator = new char[] { ',' };
            int rowsToSkip = 1;
            long lineSkipCounter = 0;


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            var client = new RestClient(string.Format("https://augeotransactions.blob.core.windows.net/mogltransactions/{0}.csv", invoice));
            var request = new RestRequest(Method.GET);
            request.AddHeader("Postman-Token", "edf5c5dd-5d2d-437a-af0f-0fe84732d81c");
            request.AddHeader("cache-control", "no-cache");
            request.AddHeader("Cookie", "mojopagesCookie=\"VisitorUID = aa7567d2 - 33a8 - 4e9b - 853b - 8aed329a5160 & LastUpdate = 16 / Aug / 2018:04:57:07 - 0700\"; _vwo_uuid_v2=D51570C5C00DDDF7CF4E643491FEF0D56|3ed74a40d247a4d27918890a1203d9f5; __utmz=103346076.1544455040.6.4.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); pid=MOGL; __utmc=103346076; __utma=103346076.86655240.1534420637.1555426039.1555432664.16; __utmt=1; __insp_wid=3338157351; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cubW9nbC5jb20vbG9naW4%2FbG9naW5fZXJyb3I9YXV0aC5iYWRjcmVkZW50aWFscyZ1c2VybmFtZT1kYW4uaW1idXJnaWFAcmV3YXJkczIxLmNvbSZzdWNjZXNzX3BhdGg9aHR0cHM6Ly93d3cubW9nbC5jb20vYWRtaW4vZGFzaGJvYXJkL2ludm9pY2VzLzI4Mjg1OQ%3D%3D; __insp_targlpt=TW9nbCAtIExvZ2lu; __insp_norec_sess=true; MOJO_AUTH=ZGFuLmltYnVyZ2lhQHJld2FyZHMyMS5jb206MTU3MDk4NDY3NjU2NTpiZjUwZWZmOTUxOWE5ODI5ZDgwZjE1YzViM2ZiMGZiMmZiZDI1NTVmNTYyMDYyYmUwNjI3ZGY3ZWQ0YThjODE4MGQyMzI4ZjNiYWM5NzJmOTNhZjY3YzgxOWE5ZTc3OGU2ZjcwZDBjNGM5ZmMwNjc5YjIwYzMyYjZiMjhjNzA5Ng; __utmb=103346076.2.10.1555432664; __insp_slim=1555432677837; sessionCookie=ef6e615f-613d-49db-b8ed-41cdd73f64bd");
            request.AddHeader("Accept-Language", "en,en-US;q=0.9");
            request.AddHeader("Accept-Encoding", "gzip, deflate, br");
            request.AddHeader("Referer", string.Format("https://augeotransactions.blob.core.windows.net/mogltransactions/{0}.csv", invoice));
            request.AddHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3");
            request.AddHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36");
            request.AddHeader("DNT", "1");
            request.AddHeader("Upgrade-Insecure-Requests", "1");
            request.AddHeader("Connection", "keep-alive");
            IRestResponse response = client.Execute(request);

            var responseString = response.Content.ToString();
            responseString = responseString.Replace("$", string.Empty);
            responseString = responseString.Replace("BANK", "0000");


            string[] csvLines = ToLines(responseString);
            Regex CSVParser = new Regex(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");
            var headers = csvLines[0].Split(fieldSeperator).ToList<string>();

            JsonResult resultSet = new JsonResult();


            foreach (var line in csvLines.Skip(rowsToSkip))
            {
                //Check to see if a line is blank.
                //This can happen on the last row if improperly terminated.
                if (line != "" || line.Trim().Length > 0)
                {
                    var lineObject = new JObject();
                    var fields = CSVParser.Split(line); // line.Split(fieldSeperator);

                    for (int x = 0; x < headers.Count; x++)
                    {
                        var propertyName = headers[x].Trim();
                        propertyName = propertyName.Replace(" ", "_");
                        fields[x] = fields[x].Replace("\"", string.Empty);
                        fields[x] = fields[x].Replace(",", string.Empty);
                        lineObject[propertyName] = fields[x];

                    }

                    resultSet.Rows.Add(lineObject);
                }
                else
                {
                    lineSkipCounter += 1;
                }
            }

            log.LogInformation(string.Format("There were {0} lines skipped, not including the header row.", lineSkipCounter));

            return (ActionResult)new OkObjectResult(resultSet);
        }

        private static string[] ToLines(string dataIn)
        {
            char[] EOLMarkerR = new char[] { '\r' };
            char[] EOLMarkerN = new char[] { '\n' };
            char[] EOLMarker = EOLMarkerR;

            //check to see if the file has both \n and \r for end of line markers.
            //common for files comming from Unix\Linux systems.
            if (dataIn.IndexOf('\n') > 0 && dataIn.IndexOf('\r') > 0)
            {
                //if we find both just remove one of them.
                dataIn = dataIn.Replace("\n", "");
            }
            //If the file only has \n then we will use that as the EOL marker to seperate the lines.
            else if (dataIn.IndexOf('\n') > 0)
            {
                EOLMarker = EOLMarkerN;
            }

            //How do we know the dynamic data will have Split capability?
            return dataIn.Split(EOLMarker);
        }

        public class JsonResult
        {
            public JsonResult()
            {
                Rows = new List<object>();

            }

            public List<object> Rows { get; set; }
        }
    }
}
