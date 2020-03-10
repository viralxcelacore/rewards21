using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Collections.Generic;

namespace functions.CreateInvoice
{
    public static class CreateInvoice
    {
        [FunctionName("CreateInvoice")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = new StreamReader(req.Body).ReadToEnd();
            // Get request body
            JObject data = null;

            try
            {
                data = JObject.Parse(requestBody);
            }
            catch(Exception ex)
            {

            }

            decimal prorate = CalculateProrate(data);
            decimal totalrevenue = CalculateRevenue(data);
            decimal netrevenue = CalculateNetRevenue(data);
            decimal repeatRevenue = CalculateRepeatRevenue(data);

            decimal adjustReturn = 0;
            decimal reward_Percent = decimal.Parse(data["accountDetails"].Value<JToken>()["Reward_Percent"].Value<string>());
            string programTerms = data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>();

            if (!string.IsNullOrEmpty(programTerms))
            {
                reward_Percent = decimal.Parse(programTerms.Split("-")[0].Split("%")[0]);
                decimal returning_Percent = decimal.Parse(programTerms.Split("-")[1].Split("%")[0]);
                adjustReturn = -returning_Percent / 100 * repeatRevenue;
            }
            else
            {
                if (data["accountDetails"].Value<JToken>()["Returning_Customer_10_Feature"].Value<string>() == "Yes")
                {
                    adjustReturn = -(reward_Percent - 10) / 100 * repeatRevenue;
                }
                else if (data["accountDetails"].Value<JToken>()["Returning_Customer_14_Feature"].Value<string>() == "Yes")
                {
                    adjustReturn = -(reward_Percent - 14) / 100 * repeatRevenue;
                }
            }

            decimal totalRewards21percent = reward_Percent / 100 * totalrevenue;
            decimal finalCharge = -prorate - adjustReturn + totalRewards21percent;

           var merchantSid = data["merchantId"].Value<string>();

            var merchantId = data["accountDetails"].Value<JToken>()["Merchant_Id"].Value<string>();

            var visits = (from row in data["transactionData"].Values<JToken>()
                          select row).Count();
                
            var invoiceNumber = "v" + merchantSid + DateTime.Now.ToString("MMyy");

            var accountType = data["accountDetails"].Value<JToken>()["Account_Type"].Value<string>();

            var psrEmailType = accountType == "Partner" ? "Regular" : "Do Not Send";

            return (ActionResult)new OkObjectResult(new {
                Name = invoiceNumber,
                Invoice_Number = invoiceNumber,
                Date = DateTime.Now.ToString("yyyy-MM-dd"),
                Amount_Due = String.Format("{0:.##}", finalCharge),
                Account_Name = merchantId,
                Revenue = String.Format("{0:.##}", totalrevenue),
                Net_Revenue = String.Format("{0:.##}", netrevenue),
                Visits = visits,
                Consumer_Views = visits,
                Link= data["invoicePDF"].Value<string>(),
                PSR_Email_Type = psrEmailType,
            });
        }

        private static decimal CalculateRepeatRevenue(JObject data)
        {
            decimal repeatRevenue = 0;
            int repeatCount = 0;

            var sortedRows = (from row in data["transactionData"].Values<JToken>()
                              orderby decimal.Parse(row["Last_4_of_cc"].Value<string>()) descending,
                                      DateTime.Parse(row["Auth_Date"].Value<string>()) ascending
                              select row);

            decimal previousCard = 0;
            DateTime previousDateTime = new DateTime();
            string programTerms = data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>();

            foreach (var row in sortedRows)
            {
                decimal currentCard = decimal.Parse(row["Last4_cc"].Value<string>());
                DateTime currentDateTime = DateTime.Parse(row["Date"].Value<string>());

                if (currentCard == previousCard)
                {
                    var value = decimal.Parse(row["NetRevenue"].Value<string>());
                    if (!string.IsNullOrEmpty(programTerms))
                    {
                        repeatCount = repeatCount + 1;

                        if (repeatCount > 2)
                        {
                            repeatRevenue += value;
                        }
                    }
                    else
                    {
                        var timespan = currentDateTime.Subtract(previousDateTime);
                        if (timespan.TotalDays < 8 && timespan.TotalDays > 0)
                        {
                            repeatRevenue += value;
                        }
                    }
                }


                previousCard = currentCard;
                previousDateTime = currentDateTime;
            }

            return repeatRevenue;
        }

        private static decimal CalculateRevenue(JObject data)
        {
            decimal totalrevenue = 0;
            totalrevenue = (from row in data["transactionData"].Values<JToken>()
                             select row
                          ).Sum(o => decimal.Parse(o["Auth_Amount"].Value<string>()));

            return totalrevenue;
        }

        private static decimal CalculateNetRevenue(JObject data)
        {
            decimal netRevenue = 0;
            netRevenue = (from row in data["transactionData"].Values<JToken>()
                            select row
                         ).Sum(o => decimal.Parse(o["Settled_Amount"].Value<string>()));

            return netRevenue;
        }

        private static decimal CalculateProrate(JObject data)
        {
            var accountType = data["accountDetails"].Value<JToken>()["Account_Type"].Value<string>();
            decimal prorate = 0;
            
            if(accountType == "Lapsed")
            {
                var dateOfLapse = data["accountDetails"].Value<JToken>()["Date_of_Lapse"].Value<string>();

                var dateOfLapseDate = DateTime.Parse(dateOfLapse);

                var rebate = (from row in data["transactionData"].Values<JToken>()
                           where DateTime.Parse(row["Auth_Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 6
                           select row
                           ).Sum(o => decimal.Parse(o["Settled_Amount"].Value<string>()));

                decimal reward_Percent = decimal.Parse(data["accountDetails"].Value<JToken>()["Reward_Percent"].Value<string>());
                prorate = reward_Percent / 100 * rebate;

            }

            return prorate;
        }
    }
}
