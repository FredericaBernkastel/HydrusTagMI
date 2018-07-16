using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace HydrusTagMI
{
  class Program
  {
    static int[] tags;
    static SQLiteConnection db;
    static string tag = "17"; // tag id or *
    static int min_p_x_y = 10; // lower limit of Pxy, -1 to disable
    static int min_x_or_y = -1; // lower limit of Px and Py, -1 to disable
    static int processing_limit = -1; // count of tags to process, -1 to all
    static string files_table = "combined_files_ac_cache_5";
    static string mappings_table = "specific_current_mappings_cache_1_5";
    static string output_db = "out.db"; // db to export the data

    static void Main(string[] args)
    {
      // open in-memory database, and attach both hydrus's master and caches db
      db = new SQLiteConnection("Data Source=:memory:; Version=3;");
      db.Open();
      SQLiteCommand cmd = db.CreateCommand();
      string sql = @"
        attach 'C:\Hydrus Network\db\client.master.db' as master;
        attach 'C:\Hydrus Network\db\client.caches.db' as caches;

        create table tmp (
            `x`             TEXT,
            `y`             TEXT,
            `Px`            INTEGER,
            `Py`            INTEGER,
            `Pxy`           INTEGER,
            `CPxy`          DOUBLE,
            `CPyx`          DOUBLE,
            `MIxy`          DOUBLE,
            `test_metric_1` DOUBLE,
            `test_metric_2` DOUBLE
        );

        select group_concat(tag_id) from master.tags";
      cmd.CommandText = sql;

      //list all tag ids
      tags = Array.ConvertAll<string,int>(cmd.ExecuteScalar().ToString().Split(','), (s) => { return int.Parse(s); });

      //count all tag pairs with specific tag
      Action<int, bool> calculate = (int tag_id, bool pairconstraint) => {
        sql = Format(@"
          select 
              a.tag_id as xid,
              b.tag_id as yid,
              (select subtag from master.subtags where subtag_id = 
                  (select subtag_id from master.tags where tag_id = a.tag_id)) as x,
              (select subtag from master.subtags where subtag_id = 
                  (select subtag_id from master.tags where tag_id = b.tag_id)) as y,
              (select namespace from master.namespaces where namespace_id = 
                  (select namespace_id from master.tags where tag_id = a.tag_id)) as namespace1,
              (select namespace from master.namespaces where namespace_id = 
                  (select namespace_id from master.tags where tag_id = b.tag_id)) as namespace2,    
              (select current_count from caches.{files_table} where tag_id = a.tag_id) as px,
              (select current_count from caches.{files_table} where tag_id = b.tag_id) as py, 
              count(*) as pxy
          from caches.{mappings_table} a
          inner join caches.{mappings_table} b
              on (a.hash_id = b.hash_id) and (a.tag_id != b.tag_id) {pairconstraint}
          where 
            a.tag_id = {tag_id}
          group by b.tag_id
          order by a.tag_id;", 

          new Dictionary<string, string>() {
            { "{pairconstraint}", pairconstraint ? "and (a.tag_id < b.tag_id)" : "" },
            { "{tag_id}",         tag_id.ToString() },
            { "{mappings_table}", mappings_table },
            { "{files_table}",    files_table }
          });

        cmd.CommandText = sql;

        //execute the query, and get data
        Task<string> task = AsyncReader(cmd, tag_id);
        task.Wait();
        string msg = task.Result;
        if (!String.IsNullOrEmpty(msg))
          Console.WriteLine(msg);
      };

      if (tag == "*") //iterate through all tags
        for (int i = 0; i < ((processing_limit > -1) ? processing_limit : tags.Length); i++)
          calculate(tags[i], true);
      else
        calculate(int.Parse(tag), false); //single tag

      // dump in-memory database to a file
      using (SQLiteConnection backupdb = new SQLiteConnection("Data Source=" + output_db + "; Version=3;"))
      {
        backupdb.Open();
        db.BackupDatabase(backupdb, "main", "main", -1, null, -1);
      }

      db.Dispose();
    }

    static async Task<string> AsyncReader(SQLiteCommand cmd, int i)
    {
      SQLiteDataReader reader = cmd.ExecuteReader();
      bool first = true;
      string msg = "";

      while (await reader.ReadAsync())
      {
        //columns to fetch
        Dictionary<string, string> data = new Dictionary<string, string>();
        Array.ForEach(
          new string[] {
            "xid", "yid", "x", "y", "namespace1", "namespace2", "px", "py", "pxy"
          }, (e) => {
            data[e] = reader[e].ToString();
          }
        );

        if (int.Parse(data["pxy"]) < min_p_x_y)
          continue;
        if ((int.Parse(data["px"]) < min_x_or_y) || (int.Parse(data["py"]) < min_x_or_y))
          continue;

        string ns1 = data["namespace1"];
        string ns2 = data["namespace2"];
        string tag1 = (String.IsNullOrEmpty(ns1) ? data["x"] : ns1 + ":" + data["x"]);
        string tag2 = (String.IsNullOrEmpty(ns2) ? data["y"] : ns2 + ":" + data["y"]);

        if (first)
          msg = "Processing tag: " + tags[i] + " | " + tag1;
        first = false;

        long Px = long.Parse(data["px"]); // count of images with tag x
        long Py = long.Parse(data["py"]); // count of images with tag y
        long Pxy = long.Parse(data["pxy"]); // count of images with both tag x and y
        double MI = MutualInformation(Px, Py, Pxy);
        double CPxy = ConditionalProbability(Py, Pxy);
        double CPyx = ConditionalProbability(Px, Pxy);


        /* write to in-memory database */

        string sql = @"
          insert into tmp
            (`x`, `y`, `Px`, `Py`, `Pxy`, `CPxy`, `CPyx`, `MIxy`, `test_metric_1`, `test_metric_2`) values
            (:x,  :y,  :Px,  :Py,  :Pxy,  :CPxy,  :CPyx,  :MI,    :metric1,        :metric2       )
        ";

        SQLiteCommand cmd2 = new SQLiteCommand(sql, db);

        List<SQLiteParameter> parameters = new List<SQLiteParameter>(){
          new SQLiteParameter(":x",   tag1       ),
          new SQLiteParameter(":y",   tag2       ),
          new SQLiteParameter(":Px",  data["px"] ),
          new SQLiteParameter(":Py",  data["py"] ),
          new SQLiteParameter(":Pxy", data["pxy"]),
          new SQLiteParameter(":CPxy",CPxy       ),
          new SQLiteParameter(":CPyx",CPyx       ),
          new SQLiteParameter(":MI",  MI         ),
          new SQLiteParameter(":metric1",  (double)Pxy / (Px + Py)),
          new SQLiteParameter(":metric2",  (double)Pxy / (Px * Py))
        };

        cmd2.Parameters.AddRange(parameters.ToArray());
        cmd2.ExecuteNonQuery();

      }
      reader.Close();
      return msg;
    }

    static double MutualInformation(long Px, long Py, long Pxy)
    {
      return Math.Log((double)Pxy / (Px * Py));
    }
    static double ConditionalProbability(long Px, long Pxy)
    {
      return (double)Pxy / Px;
    }

    static string Format(string str, Dictionary<string, string> format)
    {
      foreach (KeyValuePair<string, string> pair in format)
        str = str.Replace(pair.Key, pair.Value);
      return str;
    }
  }
}
