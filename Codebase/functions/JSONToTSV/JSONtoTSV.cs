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
using System.Text.RegularExpressions;
using System.Text;

namespace functions.JSONtoTSV
{
    public static class JSONtoTSV
    {
        [FunctionName("JSONtoTSV")]
        public static IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req, TraceWriter log)
        {

            log.Info("C# HTTP trigger function CSVToJSON processed a request.");

            string requestBody = new StreamReader(req.Body).ReadToEnd();
            // Get request body
            JObject data = JObject.Parse(requestBody);

            var orderOfItems = new List<string>() { "merchant_status", "account_id", "partnerkey", "merchant_name", "website", "category", "logo_small", "logo_medium", "logo_large", "transaction_cap", "visit_cap", "redemption_cap", "store_location_address1", "store_location_city", "store_location_state", "store_location_code", "store_location_lat", "store_location_long", "store_location_MID_type", "store_location_MID1", "store_location_MID2", "deal_type", "deal_amount", "deal_threshold", "deal_maxreward", "deal_channel", "commission_type", "commission_amount", "marketing_message", "availabilitydates" };

            var sb = new StringBuilder();

            sb.Append("Rewards-21").Append("\t");
            sb.Append(DateTime.Now.ToString("yyyyMMdd")).Append("\t");
            sb.Append(data["rows"].Count().ToString());

            sb.AppendLine();

            foreach (JObject r in data["rows"].Values<JObject>())
            {
                foreach (var property in orderOfItems)
                {
                    string propertyValue = r[property]?.Value<string>();

                    string MID1 = r["store_location_MID1"]?.Value<string>();
                    if (!MID1.Contains("E+"))
                    {
                        if (property == "marketing_message")
                        {
                            if (!string.IsNullOrEmpty(propertyValue))
                            {
                                propertyValue = propertyValue.Replace("Heartland", "VISA_MASTERCARD");
                                propertyValue = propertyValue.Replace("\"", "");
                                propertyValue = Regex.Replace(propertyValue, @"\r\n?|\n", "");
                            }
                            sb.Append(propertyValue).Append("\t");
                            continue;
                        }
                        if (property == "availabilitydates")
                        {
                            if (!string.IsNullOrEmpty(propertyValue))
                            {
                                propertyValue = propertyValue.Trim() == "|" ? "" : propertyValue;
                            }
                            sb.Append(propertyValue).Append("\t");
                            continue;
                        }
                        if (property == "deal_maxreward" || property == "deal_threshold")
                        {
                            if (!string.IsNullOrEmpty(propertyValue))
                            {
                                propertyValue = propertyValue.Replace("$", "");
                            }
                            sb.Append(propertyValue).Append("\t");

                            continue;
                        }
                        if (property == "commission_amount" || property == "deal_amount")
                        {
                            if (!string.IsNullOrEmpty(propertyValue))
                            {
                                propertyValue = Convert.ToString(Convert.ToDecimal(propertyValue) / 100);
                            }
                            else
                            {
                                propertyValue = "";
                            }
                            sb.Append(propertyValue).Append("\t");
                            continue;
                        }
                        if (property == "store_location_MID2")
                        {
                            if (string.IsNullOrEmpty(propertyValue))
                            {
                                int tabLenght = 20;
                                string tabStr = string.Empty;
                                if (!string.IsNullOrEmpty(r["store_location_MID1"]?.Value<string>()))
                                {
                                    string MID1PropValue = r["store_location_MID1"]?.Value<string>();
                                    MID1PropValue = MID1PropValue.Replace("\"", "");
                                    String[] MID1Locations = MID1PropValue.Split(',');
                                    int MID1LocationsLenght = MID1Locations.Length;
                                    tabLenght = tabLenght - (MID1LocationsLenght * 2);
                                }

                                for (int i = 0; i < tabLenght; i++)
                                {
                                    tabStr += "\t";
                                }

                                sb.Append(propertyValue).Append(tabStr);
                            }
                            else
                            {
                                sb.Append(propertyValue).Append("\t");
                            }
                            continue;
                        }
                        if (property == "store_location_address1")
                        {
                            if (!string.IsNullOrEmpty(propertyValue))
                            {
                                propertyValue = propertyValue.Replace("\"", "");
                                propertyValue = propertyValue.Replace(",", "");
                            }
                            sb.Append(propertyValue).Append("\t\t");
                            continue;
                        }
                        if (property == "store_location_MID1")
                        {
                            string propValue = r["store_location_MID1"]?.Value<string>();
                            if (!string.IsNullOrEmpty(propValue))
                            {
                                if (propValue.Contains(','))
                                {
                                    propValue = propValue.Replace("\"", "");
                                    String[] locations = propValue.Split(',');

                                    int count = 0;
                                    foreach (var location in locations)
                                    {
                                        if (count < 10)
                                        {
                                            sb.Append(r["store_location_MID_type"]?.Value<string>()).Append("\t");
                                            sb.Append(location.Trim()).Append("\t");
                                        }
                                        count++;
                                    }
                                }
                                else
                                {
                                    sb.Append(r["store_location_MID_type"]?.Value<string>()).Append("\t");
                                    sb.Append(propertyValue).Append("\t");
                                }
                            }
                            continue;
                        }
                        if (property == "store_location_MID_type")
                        {
                            continue;
                        }

                        sb.Append(propertyValue).Append("\t");
                    }
                }
                sb.AppendLine();
            }

            JsonResult resultSet = new JsonResult(sb.ToString());
            return (ActionResult)new OkObjectResult(resultSet);

        }
    }

}

