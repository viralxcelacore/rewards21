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

namespace functions.CreateInvoiceForEmpyr
{
    public static class CreateInvoiceForEmpyr
    {
        public static decimal augeoProrate = 0;
        public static decimal visaProrate = 0;
        public static decimal doshProrate = 0;
        public static decimal moglProrate = 0;

        public static decimal augeoRepeatRevenue = 0;
        public static decimal visaRepeatRevenue = 0;
        public static decimal doshRepeatRevenue = 0;
        public static decimal moglRepeatRevenue = 0;

        public static decimal mogl_Adjusted_Gross_Revenue = 0;
        public static decimal dosh_Adjusted_Gross_Revenue = 0;
        public static decimal visa_Adjusted_Gross_Revenue = 0;
        public static decimal augeo_Adjusted_Gross_Revenue = 0;


        [FunctionName("CreateInvoiceForEmpyr")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            mogl_Adjusted_Gross_Revenue = 0;
            dosh_Adjusted_Gross_Revenue = 0;
            visa_Adjusted_Gross_Revenue = 0;
            augeo_Adjusted_Gross_Revenue = 0;

            augeoProrate = 0;
            visaProrate = 0;
            doshProrate = 0;
            moglProrate = 0;

            augeoRepeatRevenue = 0;
            visaRepeatRevenue = 0;
            doshRepeatRevenue = 0;
            moglRepeatRevenue = 0;

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

            decimal total = 0;
            if (data["total"] != null && !string.IsNullOrEmpty(data["total"].ToString()))
                total = data["total"].Value<decimal>();

            decimal prorate = CalculateProrate(data);
            decimal totalrevenue = CalculateRevenue(data);
            decimal totalCustomerReward = CalculateCustomerReward(data);
            decimal totalPayPerPerformaceFee = CalculatePayPerPerformanceFee(data);
            decimal totalMarketingFee = CalculateMarketingFee(data);
            decimal netrevenue = totalrevenue - totalMarketingFee;
            decimal repeatRevenue = CalculateRepeatRevenue(data);


            decimal adjustReturn = 0;

            decimal augeoAdjustReturn = 0;
            decimal visaAdjustReturn = 0;
            decimal doshAdjustReturn = 0;
            decimal moglAdjustReturn = 0;

            decimal reward_Percent = decimal.Parse(data["accountDetails"].Value<JToken>()["Reward_Percent"].Value<string>() == "" ? "0" : data["accountDetails"].Value<JToken>()["Reward_Percent"].Value<string>());
            string programTerms = data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>();
            decimal returning_Percent = 0;

            if (!string.IsNullOrEmpty(programTerms))
            {
                if (programTerms.Contains("-"))
                {
                    reward_Percent = decimal.Parse(programTerms.Split("-")[0].Split("%")[0]);
                    returning_Percent = decimal.Parse(programTerms.Split("-")[1].Split("%")[0]);
                    adjustReturn = (reward_Percent - returning_Percent) / 100 * repeatRevenue;

                    augeoAdjustReturn = (reward_Percent - returning_Percent) / 100 * augeoRepeatRevenue;
                    visaAdjustReturn = (reward_Percent - returning_Percent) / 100 * visaRepeatRevenue;
                    doshAdjustReturn = (reward_Percent - returning_Percent) / 100 * doshRepeatRevenue;
                    moglAdjustReturn = (reward_Percent - returning_Percent) / 100 * moglRepeatRevenue;

                }
                else if (programTerms.Contains(",") && programTerms.Contains("%"))
                {
                    reward_Percent = decimal.Parse(programTerms.Split(",")[0].Split("%")[0]);
                    returning_Percent = decimal.Parse(programTerms.Split(",")[1].Split("%")[0]);
                    adjustReturn = (reward_Percent - returning_Percent) / 100 * repeatRevenue;

                    augeoAdjustReturn = (reward_Percent - returning_Percent) / 100 * augeoRepeatRevenue;
                    visaAdjustReturn = (reward_Percent - returning_Percent) / 100 * visaRepeatRevenue;
                    doshAdjustReturn = (reward_Percent - returning_Percent) / 100 * doshRepeatRevenue;
                    moglAdjustReturn = (reward_Percent - returning_Percent) / 100 * moglRepeatRevenue;
                }

            }
            else
            {
                if (data["accountDetails"].Value<JToken>()["Returning_Customer_10_Feature"].Value<string>() == "Yes")
                {
                    returning_Percent = 10;
                    adjustReturn = (reward_Percent - 10) / 100 * repeatRevenue;

                    augeoAdjustReturn = (reward_Percent - 10) / 100 * augeoRepeatRevenue;
                    visaAdjustReturn = (reward_Percent - 10) / 100  * visaRepeatRevenue;
                    doshAdjustReturn = (reward_Percent - 10) / 100  * doshRepeatRevenue;
                    moglAdjustReturn = (reward_Percent - 10) / 100 * moglRepeatRevenue;
                }
                else if (data["accountDetails"].Value<JToken>()["Returning_Customer_14_Feature"].Value<string>() == "Yes")
                {
                    returning_Percent = reward_Percent - 14;
                    adjustReturn = (14) / 100 * repeatRevenue;

                    augeoAdjustReturn = (reward_Percent - 14) / 100 * augeoRepeatRevenue;
                    visaAdjustReturn = (reward_Percent - 14) / 100 * visaRepeatRevenue;
                    doshAdjustReturn = (reward_Percent - 14) / 100 * doshRepeatRevenue;
                    moglAdjustReturn = (reward_Percent - 14) / 100 * moglRepeatRevenue;
                }
            }



            string invoice = data["invoice"].Value<string>();
            var rewardAmt = reward_Percent / 100 * totalrevenue;
            var augeoRewardAmt = reward_Percent / 100 * decimal.Parse(data["augeo_Gross_Revenue"].Value<string>().Trim());
            var visaRewardAmt = reward_Percent / 100 * decimal.Parse(data["visa_Gross_Revenue"].Value<string>().Trim());
            var doshRewardAmt = reward_Percent / 100 * decimal.Parse(data["dosh_Gross_Revenue"].Value<string>().Trim());
            var moglRewardAmt = reward_Percent / 100 * decimal.Parse(data["mogl_Gross_Revenue"].Value<string>().Trim());

            decimal finalCharge = rewardAmt - prorate - adjustReturn;

            decimal augeoFinalCharge = augeoRewardAmt - augeoProrate - augeoAdjustReturn;
            decimal visaFinalCharge = visaRewardAmt - visaProrate - visaAdjustReturn;
            decimal doshFinalCharge = doshRewardAmt - doshProrate - doshAdjustReturn;
            decimal moglFinalCharge = moglRewardAmt - moglProrate - moglAdjustReturn;


            var merchantId = data["accountDetails"].Value<JToken>()["Merchant_Id"].Value<string>();
            var merchantName = data["accountDetails"].Value<JToken>()["Merchant_Name"].Value<string>();

            var visits = (from row in data["transactionData"].Values<JToken>()
                          select row
                          ).Count();

            decimal consumerviews = 0;

            if (data["consumerviews"] != null && !string.IsNullOrEmpty(data["consumerviews"].ToString()))
                consumerviews = data["consumerviews"].Value<decimal>();

            var accountType = data["accountDetails"].Value<JToken>()["Account_Type"].Value<string>();

            var dateOfLapse = data["accountDetails"].Value<JToken>()["Date_of_Lapse"].Value<string>();
            var psrEmailType = accountType == "Partner" ? "Regular" : "Do Not Send";
            var dateOfLapseDate = new DateTime();
            if (!string.IsNullOrEmpty(dateOfLapse))
            {
                dateOfLapseDate = DateTime.Parse(dateOfLapse);
                var timelapseInMonths = DateTime.Now.Month - dateOfLapseDate.Month;

                psrEmailType = accountType == "Partner" ? "Regular" : (timelapseInMonths == 1 ? "Lapsed Last Month" : (timelapseInMonths == 2 ? "Lapsed Two Months Ago" : "Do Not Send"));
            }

            return (ActionResult)new OkObjectResult(new
            {
                Invoice_Number = invoice,
                Name = merchantName,
                Date = DateTime.Now.ToString("yyyy-MM-dd"),
                Account_Name = merchantId,
                Visits = visits,
                Consumer_Views = consumerviews,
                Customer_Reward = totalCustomerReward,
                MarketingFee = totalMarketingFee,
                Revenue = String.Format("{0:.##}", totalrevenue),
                Net_Revenue = String.Format("{0:.##}", netrevenue),
                Amount_Due = String.Format("{0:.##}", finalCharge),
                Link_to_Tx_Detail = data["invoicePDF"].Value<string>(),
                PSR_Email_Type = psrEmailType,
                AccountType = data["accountDetails"].Value<JToken>()["Account_Type"].Value<string>(),

                Mogl_Total = String.Format("{0:.##}", total),
                Dosh_total_Fees = data["doshTotalfees"].Value<string>().Trim(),
                Visa_Total_Revenue = data["visaTotalRevenue"].Value<string>().Trim(),
                Augeo_Total_Fees = data["augeoTotalFees"].Value<string>().Trim(),

                Mogl_Gross_Revenue = data["mogl_Gross_Revenue"].Value<string>().Trim(),
                Mogl_Adjusted_Gross_Revenue = data["mogl_Adjusted_Gross_Revenue"].Value<string>().Trim(),
                Dosh_Gross_Revenue = data["dosh_Gross_Revenue"].Value<string>().Trim(),
                Dosh_Adjusted_Gross_Revenue = data["dosh_Adjusted_Gross_Revenue"].Value<string>().Trim(),
                Visa_Gross_Revenue = data["visa_Gross_Revenue"].Value<string>().Trim(),
                Visa_Adjusted_Gross_Revenue = data["visa_Adjusted_Gross_Revenue"].Value<string>().Trim(),
                Augeo_Gross_Revenue = data["augeo_Gross_Revenue"].Value<string>().Trim(),
                Augeo_Adjusted_Gross_Revenue = data["augeo_Adjusted_Gross_Revenue"].Value<string>().Trim(),
                Mogl_Gross_Amount_Due = String.Format("{0:.##}", moglFinalCharge),
                Mogl_Adjusted_Gross_Amount_Due = moglFinalCharge - (decimal.Parse(data["mogl_20_5_Deduction"].Value<string>().Trim()) + decimal.Parse(data["mogl_Other_Deduction"].Value<string>().Trim())),
                Dosh_Gross_Amount_Due = String.Format("{0:.##}", doshFinalCharge),
                Dosh_Adjusted_Gross_Amount_Due = doshFinalCharge - (decimal.Parse(data["dosh_20_5_Deduction"].Value<string>().Trim()) + decimal.Parse(data["dosh_Other_Deduction"].Value<string>().Trim())),
                Visa_Gross_Amount_Due = String.Format("{0:.##}", visaFinalCharge),
                Visa_Adjusted_Gross_Amount_Due = visaFinalCharge - (decimal.Parse(data["visa_20_5_deduction"].Value<string>().Trim()) + decimal.Parse(data["visa_Other_Deduction"].Value<string>().Trim())),
                Augeo_Gross_Amount_Due = String.Format("{0:.##}", augeoFinalCharge),
                Augeo_Adjusted_Gross_Amount_Due = augeoFinalCharge - (decimal.Parse(data["augeo_20_5_Deduction"].Value<string>().Trim()) + decimal.Parse(data["augeo_Other_Deduction"].Value<string>().Trim())),
                Mogl_20_5_Deduction = decimal.Parse(data["mogl_20_5_Deduction"].Value<string>().Trim()),
                Dosh_20_5_Deduction = data["dosh_20_5_Deduction"].Value<string>().Trim(),
                Visa_20_5_deduction = data["visa_20_5_deduction"].Value<string>().Trim(),
                Augeo_20_5_Deduction = data["augeo_20_5_Deduction"].Value<string>().Trim(),
                Total_20_5_Adjustment = data["total_20_5_Adjustment"].Value<string>().Trim(),
                Mogl_Duplicate_Tx_Deduction = data["mogl_Duplicate_Tx_Deduction"].Value<string>().Trim(),
                Dosh_Duplicate_Tx_Deduction = data["dosh_Duplicate_Tx_Deduction"].Value<string>().Trim(),
                Visa_Duplicate_Tx_Deduction = data["visa_Duplicate_Tx_Deduction"].Value<string>().Trim(),
                Augeo_Duplicate_Tx_Deduction = data["augeo_Duplicate_Tx_Deduction"].Value<string>().Trim(),
                Total_Duplicate_Tx_Adj = data["total_Duplicate_Tx_Adj"].Value<string>().Trim(),
                Mogl_Other_Deduction = data["mogl_Other_Deduction"].Value<string>().Trim(),
                Dosh_Other_Deduction = data["dosh_Other_Deduction"].Value<string>().Trim(),
                Visa_Other_Deduction = data["visa_Other_Deduction"].Value<string>().Trim(),
                Augeo_Other_Deduction = data["augeo_Other_Deduction"].Value<string>().Trim(),
                TOTAL_Other_Deduction = data["total_Other_Deduction"].Value<string>().Trim(),

                Pay_per_Performance_Fee = totalPayPerPerformaceFee,
                Adjustments = (prorate + adjustReturn),
                Duplicate_Tx_Detail = data["duplicateInvoicePDF"].Value<string>().Trim(),
                Prorate = prorate,
                Program_Terms = programTerms,
                RewardPercent = reward_Percent,
                Returning_Customer_10_Feature = data["accountDetails"].Value<JToken>()["Returning_Customer_10_Feature"],
                Returning_Customer_14_Feature = data["accountDetails"].Value<JToken>()["Returning_Customer_14_Feature"],
                Returning_Percent = returning_Percent,

                AdjustReturn = adjustReturn,
                Repeat_Revenue = repeatRevenue
            });
        }

        private static decimal CalculateRepeatRevenue(JObject data)
        {
            decimal repeatRevenue = 0;
            DateTime previousDateTime = new DateTime();
            string programTerms = data["accountDetails"].Value<JToken>()["Program_Terms"].Value<string>();

            var groupedRowData = from row in data["transactionData"].Values<JToken>()
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

                                string transactionid = monthWiseTransaction["txID"].Value<string>();
                                string currentTransactionIs = transactionid.Contains("A") ? "Augeo" : (transactionid.Contains("V") ? "Visa" : (transactionid.Contains("D") ? "Dosh" : "Mogl"));


                                if (groupedRowDataByDate.Count() > 2)
                                {
                                    repeatRevenue += revenue;
                                    if (currentTransactionIs == "Augeo")
                                    {
                                        augeoRepeatRevenue += revenue;
                                    }
                                    if (currentTransactionIs == "Visa")
                                    {
                                        visaRepeatRevenue += revenue;
                                    }
                                    if (currentTransactionIs == "Dosh")
                                    {
                                        doshRepeatRevenue += revenue;
                                    }
                                    if (currentTransactionIs == "Mogl")
                                    {
                                        moglRepeatRevenue += revenue;
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
                            var revenue = decimal.Parse(trans["Revenue"].Value<string>() == "" ? "0" : trans["Revenue"].Value<string>());

                            string transactionid = trans["txID"].Value<string>();
                            string currentTransactionIs = transactionid.Contains("A") ? "Augeo" : (transactionid.Contains("V") ? "Visa" : (transactionid.Contains("D") ? "Dosh" : "Mogl"));

                            if (currentCard != 0)
                            {
                                var timespan = currentDateTime.Subtract(previousDateTime);
                                if (timespan.TotalDays < 8 && timespan.TotalDays > 0)
                                {
                                    repeatRevenue += revenue;

                                    if (currentTransactionIs == "Augeo")
                                    {
                                        augeoRepeatRevenue += revenue;
                                    }
                                    if (currentTransactionIs == "Visa")
                                    {
                                        visaRepeatRevenue += revenue;
                                    }
                                    if (currentTransactionIs == "Dosh")
                                    {
                                        doshRepeatRevenue += revenue;
                                    }
                                    if (currentTransactionIs == "Mogl")
                                    {
                                        moglRepeatRevenue += revenue;
                                    }
                                }
                                previousDateTime = currentDateTime;
                            }
                        }
                    }
                }
            }

            return repeatRevenue;
        }

        private static decimal CalculateRevenue(JObject data)
        {
            decimal totalrevenue = 0;
            totalrevenue = (from row in data["transactionData"].Values<JToken>()
                            select row
                          ).Sum(o => o["Revenue"].Value<decimal>());

            return totalrevenue;
        }

        private static decimal CalculateNetRevenue(JObject data)
        {
            decimal netRevenue = 0;
            netRevenue = (from row in data["transactionData"].Values<JToken>()
                          select row
                         ).Sum(o => o["NetRevenue"].Value<decimal>());

            return netRevenue;
        }

        private static decimal CalculateProrate(JObject data)
        {
            var accountType = data["accountDetails"].Value<JToken>()["Account_Type"].Value<string>();
            decimal prorate = 0;


            if (accountType == "Lapsed")
            {
                var dateOfLapse = data["accountDetails"].Value<JToken>()["Date_of_Lapse"].Value<string>();

                var dateOfLapseDate = DateTime.Parse(dateOfLapse);

                //-----------------------------------Total Prorate Calculation ---------------------------------------------
                prorate = (from row in data["transactionData"].Values<JToken>()
                           where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30
                           select row
                          ).Sum(o => decimal.Parse(o["PayPerPerfFee"].Value<string>()));

                prorate += (from row in data["transactionData"].Values<JToken>()
                            where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30
                            select row
                           ).Sum(o => decimal.Parse(o["CustomerRwrd"].Value<string>()));

                //-----------------------------------Augeo Prorate Calculation ---------------------------------------------
                augeoProrate = (from row in data["transactionData"].Values<JToken>()
                                where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && row["txID"].Value<string>().Contains("A")
                                select row
                         ).Sum(o => decimal.Parse(o["PayPerPerfFee"].Value<string>()));

                augeoProrate += (from row in data["transactionData"].Values<JToken>()
                                 where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && row["txID"].Value<string>().Contains("A")
                                 select row
                        ).Sum(o => decimal.Parse(o["CustomerRwrd"].Value<string>()));

                //-----------------------------------Visa Prorate Calculation ---------------------------------------------
                visaProrate = (from row in data["transactionData"].Values<JToken>()
                               where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && row["txID"].Value<string>().Contains("V")
                               select row
                        ).Sum(o => decimal.Parse(o["PayPerPerfFee"].Value<string>()));

                visaProrate += (from row in data["transactionData"].Values<JToken>()
                                where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && row["txID"].Value<string>().Contains("V")
                                select row
                        ).Sum(o => decimal.Parse(o["CustomerRwrd"].Value<string>()));

                //-----------------------------------Dosh Prorate Calculation ---------------------------------------------
                doshProrate = (from row in data["transactionData"].Values<JToken>()
                               where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && row["txID"].Value<string>().Contains("D")
                               select row
                        ).Sum(o => decimal.Parse(o["PayPerPerfFee"].Value<string>()));

                doshProrate += (from row in data["transactionData"].Values<JToken>()
                                where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && row["txID"].Value<string>().Contains("D")
                                select row
                        ).Sum(o => decimal.Parse(o["CustomerRwrd"].Value<string>()));

                //-----------------------------------Mogl Prorate Calculation ---------------------------------------------
                moglProrate = (from row in data["transactionData"].Values<JToken>()
                               where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && (!row["txID"].Value<string>().Contains("A") || !row["txID"].Value<string>().Contains("D") || !row["txID"].Value<string>().Contains("V"))
                               select row
                        ).Sum(o => decimal.Parse(o["PayPerPerfFee"].Value<string>()));

                moglProrate = (from row in data["transactionData"].Values<JToken>()
                               where DateTime.Parse(row["Date"].Value<string>()).Subtract(dateOfLapseDate).TotalDays > 30 && (!row["txID"].Value<string>().Contains("A") || !row["txID"].Value<string>().Contains("D") || !row["txID"].Value<string>().Contains("V"))
                               select row
                        ).Sum(o => decimal.Parse(o["CustomerRwrd"].Value<string>()));


            }

            return prorate;
        }

        private static decimal CalculateCustomerReward(JObject data)
        {
            decimal customerReward = 0;

            customerReward = (from row in data["transactionData"].Values<JToken>()
                              select row
                     ).Sum(o => decimal.Parse(o["CustomerRwrd"].Value<string>()));

            return customerReward;
        }

        private static decimal CalculateMarketingFee(JObject data)
        {
            decimal marketingFee = 0;

            marketingFee = (from row in data["transactionData"].Values<JToken>()
                            select row
                   ).Sum(o => decimal.Parse(o["Mrkt_Fee"].Value<string>()));

            return marketingFee;
        }

        private static decimal CalculatePayPerPerformanceFee(JObject data)
        {
            decimal payPerPerformaceFee = 0;

            payPerPerformaceFee = (from row in data["transactionData"].Values<JToken>()
                                   select row
                     ).Sum(o => decimal.Parse(o["PayPerPerfFee"].Value<string>()));

            return payPerPerformaceFee;
        }
    }
}
