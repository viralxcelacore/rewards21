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
using MoreLinq.Extensions;
using MoreLinq;
using System.Collections.Generic;

namespace functions
{
    public static class DeDuplicateTransactions
    {
        public static List<JToken> duplicateRows = new List<JToken>();
        public static List<JToken> uniqueData = new List<JToken>();
        public static List<JToken> finalData = new List<JToken>();
        public static List<JToken> AugeoReport = new List<JToken>(); //Augeo report for duplicate transaction and 5% charged transactions

        public static decimal Mogl_Duplicate_Tx_Deduction = 0;
        public static decimal Dosh_Duplicate_Tx_Deduction = 0;
        public static decimal Visa_Duplicate_Tx_Deduction = 0;
        public static decimal Augeo_Duplicate_Tx_Deduction = 0;

        public static decimal Mogl_Gross_Revenue = 0;
        public static decimal Dosh_Gross_Revenue = 0;
        public static decimal Visa_Gross_Revenue = 0;
        public static decimal Augeo_Gross_Revenue = 0;

        public static decimal Mogl_Adjusted_Gross_Revenue = 0;
        public static decimal Dosh_Adjusted_Gross_Revenue = 0;
        public static decimal Visa_Adjusted_Gross_Revenue = 0;
        public static decimal Augeo_Adjusted_Gross_Revenue = 0;


        [FunctionName("DeDuplicateTransactions")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            log.LogInformation("C# HTTP trigger function processed a request.");
            duplicateRows = new List<JToken>();
            uniqueData = new List<JToken>();
            finalData = new List<JToken>();
            AugeoReport = new List<JToken>();

            decimal doshTotalfees = 0;
            decimal visaTotalRevenue = 0;
            decimal augeoTotalFees = 0;
            decimal augeoTotalPayPerPerfFee = 0;
            decimal augeoTotalRevenue = 0;
            decimal augeoTotalMrkt_Fee = 0;
            decimal augeoTotalNetRevenue = 0;

            decimal Mogl_20_5_Deduction = 0;
            decimal Dosh_20_5_Deduction = 0;
            decimal Visa_20_5_deduction = 0;
            decimal Augeo_20_5_Deduction = 0;

            decimal Mogl_Other_Deduction = 0;
            decimal Dosh_Other_Deduction = 0;
            decimal Visa_Other_Deduction = 0;
            decimal Augeo_Other_Deduction = 0;

            Mogl_Gross_Revenue = 0;
            Dosh_Gross_Revenue = 0;
            Visa_Gross_Revenue = 0;
            Augeo_Gross_Revenue = 0;

            Mogl_Adjusted_Gross_Revenue = 0;
            Dosh_Adjusted_Gross_Revenue = 0;
            Visa_Adjusted_Gross_Revenue = 0;
            Augeo_Adjusted_Gross_Revenue = 0;

            Mogl_Duplicate_Tx_Deduction = 0;
            Dosh_Duplicate_Tx_Deduction = 0;
            Visa_Duplicate_Tx_Deduction = 0;
            Augeo_Duplicate_Tx_Deduction = 0;


            string requestBody = new StreamReader(req.Body).ReadToEnd();
            // Get request body
            JObject data = null;

            try
            {
                data = JObject.Parse(requestBody);
            }
            catch (Exception ex)
            {

            }


            string Merchant_Id = data["accountDetails"]["Merchant_Id"].Value<string>();
            string Merchant_Name = data["accountDetails"]["Merchant_Name"].Value<string>();

            var dataRows = (from row in data["transactionData"].Values<JToken>()
                            select row);



            foreach (var dataRow in dataRows)
            {
                string TXID = dataRow["txID"].Value<string>();
                if (TXID.Contains("D"))
                {
                    doshTotalfees += decimal.Parse(dataRow["PayPerPerfFee"].Value<string>());
                }
                if (TXID.Contains("V"))
                {
                    visaTotalRevenue += decimal.Parse(dataRow["Revenue"].Value<string>());
                }
                if (TXID.Contains("A"))
                {
                    augeoTotalFees += decimal.Parse(dataRow["PayPerPerfFee"].Value<string>());
                }
            }


            removeDuplicates(data);

            decimal reward_Percent = decimal.Parse(data["accountDetails"].Value<JToken>()["Reward_Percent"].Value<string>() == "" ? "0" : data["accountDetails"].Value<JToken>()["Reward_Percent"].Value<string>());
            string programTerms = data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>();
            decimal returning_Percent = 0;

            if (!string.IsNullOrEmpty(programTerms))
            {
                if (programTerms.Contains("-"))
                {
                    reward_Percent = decimal.Parse(programTerms.Split("-")[0].Split("%")[0]);
                    returning_Percent = decimal.Parse(programTerms.Split("-")[1].Split("%")[0]);

                }
                else if (programTerms.Contains(",") && programTerms.Contains("%"))
                {
                    reward_Percent = decimal.Parse(programTerms.Split(",")[0].Split("%")[0]);
                    returning_Percent = decimal.Parse(programTerms.Split(",")[1].Split("%")[0]);
                }

            }
            else
            {
                if (data["accountDetails"].Value<JToken>()["Returning_Customer_10_Feature"].Value<string>() == "Yes")
                {
                    returning_Percent = 10;
                }
                else if (data["accountDetails"].Value<JToken>()["Returning_Customer_14_Feature"].Value<string>() == "Yes")
                {
                    var returnPercent = reward_Percent - 14;
                    returning_Percent = returnPercent > 0 ? returnPercent : 0;
                }
            }

            var repeatCount = 0;

            decimal previousCard = 0;
            DateTime previousDateTime = new DateTime();

            var groupedRowData = from row in uniqueData
                                 group row by row["Last4_cc"]
                                 into g
                                 select new { groupedProperty = g.Key, groupedList = g.ToList() };


            foreach (var row in groupedRowData)
            {

                if (row.groupedList.Count > 1)
                {
                    var sortedRows = (from sortedrow in row.groupedList
                                      orderby DateTime.Parse(sortedrow["Date"].Value<string>()) ascending
                                      select sortedrow);

                    if (!string.IsNullOrEmpty(programTerms))
                    {

                        Dictionary<int, List<JToken>> monthWiseTransactions = new Dictionary<int, List<JToken>>();

                        foreach (var monthRow in sortedRows)
                        {
                            DateTime currentDateTime = DateTime.Parse(monthRow["Date"].Value<string>());
                            if (monthWiseTransactions.ContainsKey(currentDateTime.Month))
                            {
                                monthWiseTransactions.GetValueOrDefault(currentDateTime.Month).Add(monthRow);
                            }
                            else
                            {
                                monthWiseTransactions.Add(currentDateTime.Month, new List<JToken>() { monthRow });
                            }
                        }

                        foreach (var month in monthWiseTransactions.Keys)
                        {
                            var groupedRowDataByDate = from daterow in monthWiseTransactions.GetValueOrDefault(month)
                                                       group daterow by daterow["Date"]
                                                      into g
                                                       select new { groupedProperty = g.Key, groupedList = g.ToList() };


                            foreach (var monthWiseTransaction in monthWiseTransactions.GetValueOrDefault(month))
                            {
                                var revenue = decimal.Parse(monthWiseTransaction["Revenue"].Value<string>() == "" ? "0" : monthWiseTransaction["Revenue"].Value<string>());
                                decimal mrktFee = 0;
                                bool IsAugeoRepeated = false;
                                bool IsVisaRepeated = false;
                                bool IsDoshRepeated = false;
                                bool IsMOGLRepeated = false;

                                if (groupedRowDataByDate.Count() > 2)
                                {
                                    mrktFee = returning_Percent / 100 * revenue;

                                    string transactionid = monthWiseTransaction["txID"].Value<string>();
                                    string currentTransactionIs = transactionid.Contains("A") ? "Augeo" : (transactionid.Contains("V") ? "Visa" : (transactionid.Contains("D") ? "Dosh" : "Mogl"));

                                    if (currentTransactionIs == "Augeo")
                                    {
                                        IsAugeoRepeated = true;
                                    }
                                    if (currentTransactionIs == "Visa")
                                    {
                                        IsVisaRepeated = true;
                                    }
                                    if (currentTransactionIs == "Dosh")
                                    {
                                        IsDoshRepeated = true;
                                    }
                                    if (currentTransactionIs == "Mogl")
                                    {
                                        IsMOGLRepeated = true;
                                    }

                                }
                                else
                                {
                                    mrktFee = reward_Percent / 100 * revenue;
                                }

                                monthWiseTransaction["Mrkt_Fee"] = Math.Round(mrktFee, 2, MidpointRounding.AwayFromZero) > 500 ? 500 : Math.Round(mrktFee, 2, MidpointRounding.AwayFromZero);
                                monthWiseTransaction["NetRevenue"] = Math.Round(revenue - mrktFee, 2, MidpointRounding.AwayFromZero);
                                monthWiseTransaction["Date"] = DateTime.Parse(monthWiseTransaction["Date"].Value<string>()).ToString("M/d/yyyy");
                                finalData.Add(monthWiseTransaction);


                                if (!string.IsNullOrEmpty(programTerms)) // "20% Standard - 5% on Returning Visitors"
                                {
                                    if (programTerms.Contains("-"))
                                    {
                                        if (IsAugeoRepeated)
                                        {
                                            AugeoReport.Add(monthWiseTransaction);

                                            Augeo_20_5_Deduction += revenue;
                                        }
                                        if (IsVisaRepeated)
                                        {
                                            Visa_20_5_deduction += revenue;
                                        }
                                        if (IsDoshRepeated)
                                        {
                                            Dosh_20_5_Deduction += revenue;
                                        }
                                        if (IsMOGLRepeated)
                                        {
                                            Mogl_20_5_Deduction += revenue;
                                        }
                                    }
                                }
                                else // "Returning Customer 10 or 14 Feature"
                                {
                                    if (IsAugeoRepeated)
                                    {
                                        Augeo_Other_Deduction += revenue;
                                    }
                                    if (IsVisaRepeated)
                                    {
                                        Visa_Other_Deduction += revenue;
                                    }
                                    if (IsDoshRepeated)
                                    {
                                        Dosh_Other_Deduction += revenue;
                                    }
                                    if (IsMOGLRepeated)
                                    {
                                        Mogl_Other_Deduction += revenue;
                                    }
                                }


                            }
                        }
                    }
                    else
                    {
                        foreach (var trans in sortedRows)
                        {
                            decimal currentCard = decimal.Parse(trans["Last4_cc"].Value<string>() == "" ? "0" : trans["Last4_cc"].Value<string>());
                            DateTime currentDateTime = DateTime.Parse(trans["Date"].Value<string>());
                            var revenue = decimal.Parse(trans["Revenue"].Value<string>());
                            decimal mrktFee = 0;

                            if (currentCard != 0)
                            {
                                var timespan = currentDateTime.Subtract(previousDateTime);
                                if (timespan.TotalDays < 8 && timespan.TotalDays > 0 && returning_Percent != 0)
                                {
                                    mrktFee = returning_Percent / 100 * revenue;
                                }
                                else
                                {
                                    mrktFee = reward_Percent / 100 * revenue;
                                }
                                previousDateTime = currentDateTime;
                            }
                            else
                            {
                                mrktFee = reward_Percent / 100 * revenue;
                            }

                            trans["Mrkt_Fee"] = Math.Round(mrktFee, 2, MidpointRounding.AwayFromZero) > 500 ? 500 : Math.Round(mrktFee, 2, MidpointRounding.AwayFromZero);
                            trans["NetRevenue"] = Math.Round(revenue - mrktFee, 2, MidpointRounding.AwayFromZero);
                            trans["Date"] = DateTime.Parse(trans["Date"].Value<string>()).ToString("M/d/yyyy");
                            finalData.Add(trans);
                        }
                    }

                }
                else
                {

                    foreach (var trans in row.groupedList)
                    {
                        decimal currentCard = decimal.Parse(trans["Last4_cc"].Value<string>() == "" ? "0" : trans["Last4_cc"].Value<string>());
                        DateTime currentDateTime = DateTime.Parse(trans["Date"].Value<string>());
                        var revenue = decimal.Parse(trans["Revenue"].Value<string>());
                        decimal mrktFee = 0;

                        mrktFee = reward_Percent / 100 * revenue;
                        trans["Mrkt_Fee"] = Math.Round(mrktFee, 2, MidpointRounding.AwayFromZero) > 500 ? 500 : Math.Round(mrktFee, 2, MidpointRounding.AwayFromZero);
                        trans["NetRevenue"] = Math.Round(revenue - mrktFee, 2, MidpointRounding.AwayFromZero);
                        trans["Date"] = DateTime.Parse(trans["Date"].Value<string>()).ToString("M/d/yyyy");
                        finalData.Add(trans);
                    }

                }
            }


            finalData = (from row in finalData
                         orderby DateTime.Parse(row["Date"].Value<string>()) ascending
                         select row).ToList();


            foreach (var row in AugeoReport)
            {
                if (row["PayPerPerfFee"] != null)
                {
                    augeoTotalPayPerPerfFee += decimal.Parse(row["PayPerPerfFee"].Value<string>() == "" ? "0" : row["PayPerPerfFee"].Value<string>());
                }
                if (row["Revenue"] != null)
                {
                    augeoTotalRevenue += decimal.Parse(row["Revenue"].Value<string>() == "" ? "0" : row["Revenue"].Value<string>());
                }
                if (row["Mrkt_Fee"] != null)
                {
                    augeoTotalMrkt_Fee += decimal.Parse(row["Mrkt_Fee"].Value<string>() == "" ? "0" : row["Mrkt_Fee"].Value<string>());
                }
                if (row["NetRevenue"] != null)
                {
                    augeoTotalNetRevenue += decimal.Parse(row["NetRevenue"].Value<string>() == "" ? "0" : row["NetRevenue"].Value<string>());
                }
            }




            return (ActionResult)new OkObjectResult(new
            {

                deDuplicateTransactions = finalData,
                duplicateRows = duplicateRows,
                doshTotalfees = doshTotalfees,
                visaTotalRevenue = visaTotalRevenue,
                augeoTotalFees = augeoTotalFees,
                augeoReport = AugeoReport,
                merchant_Id = Merchant_Id,
                merchant_Name = Merchant_Name,
                augeoTotalPayPerPerfFee = augeoTotalPayPerPerfFee,
                augeoTotalRevenue = augeoTotalRevenue,
                augeoTotalMrkt_Fee = augeoTotalMrkt_Fee,
                augeoTotalNetRevenue = augeoTotalNetRevenue,
                //
                mogl_Gross_Revenue = Mogl_Gross_Revenue,
                mogl_Adjusted_Gross_Revenue = Mogl_Adjusted_Gross_Revenue,
                dosh_Gross_Revenue = Dosh_Gross_Revenue,
                dosh_Adjusted_Gross_Revenue = Dosh_Adjusted_Gross_Revenue,
                visa_Gross_Revenue = Visa_Gross_Revenue,
                visa_Adjusted_Gross_Revenue = Visa_Adjusted_Gross_Revenue,
                augeo_Gross_Revenue = Augeo_Gross_Revenue,
                augeo_Adjusted_Gross_Revenue = Augeo_Adjusted_Gross_Revenue,
                mogl_Gross_Amount_Due = 0,
                mogl_Adjusted_Gross_Amount_Due = 0,
                dosh_Gross_Amount_Due = 0,
                dosh_Adjusted_Gross_Amount_Due = 0,
                visa_Gross_Amount_Due = 0,
                visa_Adjusted_Gross_Amount_Due = 0,
                augeo_Gross_Amount_Due = 0,
                augeo_Adjusted_Gross_Amount_Due = 0,
                mogl_20_5_Deduction = Mogl_20_5_Deduction,
                dosh_20_5_Deduction = Dosh_20_5_Deduction,
                visa_20_5_deduction = Visa_20_5_deduction,
                augeo_20_5_Deduction = Augeo_20_5_Deduction,
                total_20_5_Adjustment = (Mogl_20_5_Deduction + Dosh_20_5_Deduction + Visa_20_5_deduction + Augeo_20_5_Deduction),
                mogl_Duplicate_Tx_Deduction = Mogl_Duplicate_Tx_Deduction,
                dosh_Duplicate_Tx_Deduction = Dosh_Duplicate_Tx_Deduction,
                visa_Duplicate_Tx_Deduction = Visa_Duplicate_Tx_Deduction,
                augeo_Duplicate_Tx_Deduction = Augeo_Duplicate_Tx_Deduction,
                total_Duplicate_Tx_Adj = (Mogl_Duplicate_Tx_Deduction + Dosh_Duplicate_Tx_Deduction + Visa_Duplicate_Tx_Deduction + Augeo_Duplicate_Tx_Deduction),
                mogl_Other_Deduction = Mogl_Other_Deduction,
                dosh_Other_Deduction = Dosh_Other_Deduction,
                visa_Other_Deduction = Visa_Other_Deduction,
                augeo_Other_Deduction = Augeo_Other_Deduction,
                total_Other_Deduction = (Mogl_Other_Deduction + Dosh_Other_Deduction + Visa_Other_Deduction + Augeo_Other_Deduction)
            });                           
                                          
        }                               

        private static void removeDuplicates(JObject data)
        {
            if (data != null)
            {
                var sortedRows = (from row in data["transactionData"].Values<JToken>()
                                  orderby decimal.Parse(row["Revenue"].Value<string>()) descending,
                                  decimal.Parse(row["Last4_cc"].Value<string>() == "" ? "0" : row["Last4_cc"].Value<string>()) descending,
                                  DateTime.Parse(row["Date"].Value<string>()) ascending
                                  select row);

                decimal previousAmount = 0, previousCard = 0;
                var previousDate = new DateTime();
                var previousTransactionIs = string.Empty;

                foreach (var row in sortedRows)
                {
                    decimal currentAmount = decimal.Parse(row["Revenue"].Value<string>() == "" ? "0" : row["Revenue"].Value<string>());
                    DateTime currentDateTime = DateTime.Parse(row["Date"].Value<string>());
                    decimal currentCard = decimal.Parse(row["Last4_cc"].Value<string>() == "" ? "0" : row["Last4_cc"].Value<string>());
                    string transactionid = row["txID"].Value<string>();
                    string currentTransactionIs = transactionid.Contains("A") ? "Augeo" : (transactionid.Contains("V") ? "Visa" : (transactionid.Contains("D") ? "Dosh" : "Mogl"));

                    string programTerm = string.Empty;


                    if (!string.IsNullOrEmpty(data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>()))
                    {
                        programTerm = data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>();
                        // programTerm.Contains("-");
                    }



                    if (currentAmount == previousAmount && (currentDateTime - previousDate).TotalDays <= 7 && currentCard == previousCard /*&& previousTransactionIs != currentTransactionIs*/)
                    {
                        duplicateRows.Add(row);

                        if (currentTransactionIs == "Augeo")
                        {
                            AugeoReport.Add(row);
                            Augeo_Duplicate_Tx_Deduction += currentAmount;
                        }
                        if (currentTransactionIs == "Visa")
                        {
                            Visa_Duplicate_Tx_Deduction += currentAmount;
                        }
                        if (currentTransactionIs == "Dosh")
                        {
                            Dosh_Duplicate_Tx_Deduction += currentAmount;
                        }
                        if (currentTransactionIs == "Mogl")
                        {
                            Mogl_Duplicate_Tx_Deduction += currentAmount;
                        }
                    }
                    else
                    {
                        uniqueData.Add(row);

                        if (currentTransactionIs == "Augeo")
                        {
                            Augeo_Adjusted_Gross_Revenue += currentAmount;
                        }
                        if (currentTransactionIs == "Visa")
                        {
                            Visa_Adjusted_Gross_Revenue += currentAmount;
                        }
                        if (currentTransactionIs == "Dosh")
                        {
                            Dosh_Adjusted_Gross_Revenue += currentAmount;
                        }
                        if (currentTransactionIs == "Mogl")
                        {
                            Mogl_Adjusted_Gross_Revenue += currentAmount;
                        }

                    }

                    if (currentTransactionIs == "Augeo")
                    {
                        Augeo_Gross_Revenue += currentAmount;
                    }
                    if (currentTransactionIs == "Visa")
                    {
                        Visa_Gross_Revenue += currentAmount;
                    }
                    if (currentTransactionIs == "Dosh")
                    {
                        Dosh_Gross_Revenue += currentAmount;
                    }
                    if (currentTransactionIs == "Mogl")
                    {
                        Mogl_Gross_Revenue += currentAmount;
                    }

                    previousAmount = currentAmount;
                    previousDate = currentDateTime;
                    previousCard = currentCard;
                    previousTransactionIs = currentTransactionIs;
                }
            }
        }
    }
}