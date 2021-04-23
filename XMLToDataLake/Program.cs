using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Xml;

using Microsoft.Extensions.Configuration;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Threading.Tasks;

namespace TransferToDatalake
{
    class Program
    {
        private static IConfiguration _configuration;
        static void Main(string[] args)
        {
            //Steps to get the connection string from the appsettings.json file
            GetAppSettingsFile();

            BlobContainerClient container = new BlobContainerClient(_configuration.GetConnectionString("BlobStorageConnectionString"), "raw");
            using (SqlConnection con = new SqlConnection(_configuration.GetConnectionString("DatabaseConnectionString")))
            {
                LoadDataAsync(container, con).Wait();
            }

        }


        async static Task LoadDataAsync(BlobContainerClient container, SqlConnection con)
        {
            con.Open();
            int rowcount = 0;
            bool continueloop = true;
            int rowlimit = 1;
            int offset = 0;
            while (continueloop)
            {
                //The query to get your XML data, note the offset/Fetch syntax to get one row at a time, make sure to use a valid sort column,
                //using FOR XML PATH with a specified root to create a valid XML document from the query
                SqlCommand cmd = new SqlCommand("SELECT * FROM XMLTestTable ORDER BY info1 OFFSET " + offset.ToString() + " ROWS FETCH FIRST " + rowlimit.ToString() + " ROWS ONLY FOR XML PATH, ROOT('xmlrows')", con);

                using (XmlReader xml = await cmd.ExecuteXmlReaderAsync())
                {
                    //Reading all XML in one read and converting to bytes
                    xml.Read();
                    var content = Encoding.UTF8.GetBytes(xml.ReadOuterXml());
                    
                    if (content.Length > 0)
                    {
                        //Second query gets metadata to name the file
                        SqlCommand cmd2 = new SqlCommand("SELECT info1, info2 FROM XMLTestTable ORDER BY info1 OFFSET " + offset.ToString() + " ROWS FETCH FIRST " + rowlimit.ToString() + " ROWS ONLY", con);
                        using (SqlDataReader data = await cmd2.ExecuteReaderAsync())
                        {
                            data.Read();
                            using (var ms = new MemoryStream(content))
                            {
                                //Name file based on information in second query, we could also reparse the XML above to get the data
                                BlobClient blob = container.GetBlobClient("/xml/" + data[0].ToString() + "_" + data[1].ToString() + ".xml");

                                //Upload to data lake, don't wait for the thread to complete for speed
                                blob.UploadAsync(ms, new BlobHttpHeaders { ContentType = "application/xml" }, conditions: null);
                            }
                            rowcount++;

                            Console.WriteLine("Uploaded XML: " + rowcount);

                            offset = offset + rowlimit;
                        }
                    }
                    else
                    {
                        //If our XML is empty break out of the loop
                        continueloop = false;
                    }
                }
            }
        }

        //Default process for getting configuration settings from appsettings.json
        static void GetAppSettingsFile()
        {
            var builder = new ConfigurationBuilder()
                                 .SetBasePath(Directory.GetCurrentDirectory())
                                 .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            _configuration = builder.Build();
        }
    }
}
