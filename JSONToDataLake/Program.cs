using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Xml;

using Microsoft.Extensions.Configuration;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace JSONToDataLake
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
            //Open the SQL Connection
            con.Open();

            //Settings for number of rows per run currently this is built to only handle one row at a time
            int rowlimit = 1;
            int offset = 0;

            //Initialize control variables
            int rowcount = 0;
            bool continueloop = true;
            
            while (continueloop)
            {
                //The query to get your metadata and JSON data, note the offset/Fetch syntax to get one row at a time, make sure to use a valid sort column
                SqlCommand cmd = new SqlCommand("SELECT info1, info2, json_text FROM JSONTestTable ORDER BY info1 OFFSET " + offset.ToString() + " ROWS FETCH FIRST " + rowlimit.ToString() + " ROWS ONLY", con);

                using (SqlDataReader data = await cmd.ExecuteReaderAsync())
                {
                    if (data.Read())
                    {
                        //Turn metadata and original JSON into an object
                        dynamic output_object = new
                        {
                            //You'll want to add a new item for each piece of metadata you want included in the JSON document
                            info1 = data.GetInt32(0),
                            info2 = data.GetString(1),
                            //Convert the JSON to an object
                            json = JsonConvert.DeserializeObject(data.GetString(2))
                    };

                        //Convert the object to a string, encode it as UTF8, and turn it into a memory stream
                        using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(output_object))))
                        {
                            //Using some of the metadata to create a unique file name, this will be put in the JSON folder
                            BlobClient blob = container.GetBlobClient("/json/" + output_object.info1 + "_" + output_object.info2.ToString() + ".json");

                            //Upload to data lake, don't wait for the thread to complete for speed
                            blob.UploadAsync(ms, new BlobHttpHeaders { ContentType = "application/json" }, conditions: null);
                        }
                        rowcount++;

                        Console.WriteLine("Uploaded JSON: " + rowcount);

                        offset = offset + rowlimit;
                    }
                    else
                    {
                        //If our data reader is empty data.Read() above, break out of the loop
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
