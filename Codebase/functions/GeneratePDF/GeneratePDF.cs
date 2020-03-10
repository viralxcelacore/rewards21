using iTextSharp.text;
using iTextSharp.text.pdf;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace functions.GeneratePDF
{
    public static class GeneratePDF
    {
        [FunctionName("GeneratePDF")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");


            string requestBody = new StreamReader(req.Body).ReadToEnd();
            // Get request body
            JObject data = JObject.Parse(requestBody);

            var pdfDoc = new Document(PageSize.A4);


            MemoryStream memoryStream = new MemoryStream();
            var pdfWriter = PdfWriter.GetInstance(pdfDoc, memoryStream);

            pdfDoc.AddAuthor("Rewards 21");
            pdfDoc.Open();

          
            iTextSharp.text.Font font7white = iTextSharp.text.FontFactory.GetFont(FontFactory.COURIER_BOLD, 7, BaseColor.White);
            iTextSharp.text.Font font7 = iTextSharp.text.FontFactory.GetFont(FontFactory.COURIER_BOLD, 7, BaseColor.Black);

            var firstRow = (from transaction in data["rows"].Values<JObject>()
                            select transaction).FirstOrDefault();
            var properties = firstRow.Properties().Select(o => o.Name);

            var orderOfItems = new List<string>() { "txID", "Date", "Last4_cc", "Revenue", "Mrkt_Fee", "NetRevenue" };
            PdfPTable table = new PdfPTable(properties.Count());
           
            table.WidthPercentage = 100;
            int iCol = 0;
            string colname = "";

            foreach (var property in orderOfItems)
            {
                table.AddCell(new PdfPCell(new Phrase(property, font7white)) { BackgroundColor = new BaseColor(91, 190, 222) });
            }

            foreach (JObject r in data["rows"].Values<JObject>())
            {
                foreach (var property in orderOfItems)
                {
                    table.AddCell(new PdfPCell(new Phrase(r[property].Value<string>(), font7)) { BackgroundColor = BaseColor.LightGray });
                }
            }

            pdfDoc.Add(table);
            pdfDoc.Close();
            byte[] pdfcontent = memoryStream.ToArray();
            memoryStream.Dispose();




            return new FileContentResult(pdfcontent, "application/pdf");
        }
    }
}

