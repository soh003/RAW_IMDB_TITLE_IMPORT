using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Collections.Generic;

class Program
{
    static void Main()
    {
        string filePath = @"C:\temp\title.basics.tsv"; // IMDb-filens placering
        string connectionString = "Server=localhost;Database=IMDB_DB;Integrated Security=True;"; // Tilpas til din SQL Server

        Console.WriteLine("Starter import...");

        List<string> droppedRows = new List<string>(); // Liste til droppede rækker
        int count = 0;          // Flyttet ud her
        int skippedCount = 0;   // Flyttet ud her

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Truncate titles_raw før import
            using (SqlCommand cmd = new SqlCommand("TRUNCATE TABLE titles_raw;", connection))
            {
                cmd.ExecuteNonQuery();
                Console.WriteLine("titles_raw er nulstillet.");
            }

            // Opret DataTable til bulk copy
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("tconst", typeof(string));
            dataTable.Columns.Add("titleType", typeof(string));
            dataTable.Columns.Add("primaryTitle", typeof(string));
            dataTable.Columns.Add("originalTitle", typeof(string));
            dataTable.Columns.Add("isAdult", typeof(bool));
            dataTable.Columns.Add("startYear", typeof(int));
            dataTable.Columns.Add("endYear", typeof(int));
            dataTable.Columns.Add("runtimeMinutes", typeof(int));
            dataTable.Columns.Add("genres", typeof(string));

            int batchSize = 10000; // Antal rækker pr. batch

            using (StreamReader reader = new StreamReader(filePath))
            {
                reader.ReadLine(); // Spring header-linjen over

                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    string[] columns = line.Split('\t');

                    if (columns.Length < 9)
                    {
                        droppedRows.Add(line);
                        skippedCount++;
                        continue;
                    }

                    try
                    {
                        dataTable.Rows.Add(
                            columns[0],   // tconst
                            ParseNullableString(columns[1]),   // titleType
                            ParseNullableString(columns[2]),   // primaryTitle
                            ParseNullableString(columns[3]),   // originalTitle
                            columns[4] == "1",  // isAdult
                            ParseNullableIntDefault(columns[5]), // startYear
                            ParseNullableIntDefault(columns[6]), // endYear
                            ParseNullableIntDefault(columns[7]), // runtimeMinutes
                            ParseNullableString(columns[8])    // genres
                        );
                    }
                    catch (Exception)
                    {
                        droppedRows.Add(line);
                        skippedCount++;
                    }

                    count++;

                    // Indsæt data i SQL, når vi når batchSize
                    if (count % batchSize == 0)
                    {
                        BulkInsert(connection, dataTable);
                        dataTable.Clear();
                        Console.WriteLine($"{count} rækker indsat... Skippet: {skippedCount}");
                    }
                }

                // Indsæt resterende rækker
                if (dataTable.Rows.Count > 0)
                {
                    BulkInsert(connection, dataTable);
                    Console.WriteLine("Sidste batch indsat.");
                }
            }
        }

        // Opsummering af importen
        Console.WriteLine($"\n Import færdig! {count} rækker forsøgt indsat, {skippedCount} rækker blev skippet.");

        if (skippedCount > 0)
        {
            Console.WriteLine("\n Følgende rækker blev droppet:");
            foreach (var row in droppedRows)
            {
                Console.WriteLine(row);
            }
        }
    }

    // Bulk insert-funktion
    static void BulkInsert(SqlConnection connection, DataTable dataTable)
    {
        try
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "titles_raw";
                bulkCopy.BatchSize = dataTable.Rows.Count;
                bulkCopy.WriteToServer(dataTable);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($" Fejl under bulk insert: {ex.Message}");
        }
    }

    // Håndtering af `\N` i string-felter og erstatter `|` med `,`
    static string ParseNullableString(string value)
    {
        return value == "\\N" ? null : value.Replace("|", ",");
    }

    // Håndtering af `\N` i int-felter, erstatter `\N` med 0
    static int ParseNullableIntDefault(string value)
    {
        return value == "\\N" ? 0 : int.TryParse(value, out int result) ? result : 0;
    }
}
