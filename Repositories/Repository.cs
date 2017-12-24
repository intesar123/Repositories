using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Repositories
{
    public class Repository
    {
        //public MySqlConnection mysqlcon;
        //public SqlConnection mssqlcon;
        #region Database Section
        public static int ismssql = Convert.ToInt32(ConfigurationManager.AppSettings["DBTYPE"]);
        public static string dbname = string.Empty;
        public static string dbserver = string.Empty;
        
        private static SqlConnection getMSSqlConnection(string ConnName = "")
        {

            SqlConnection con = new SqlConnection(Repository.getConnectionString(ConnName));
            return con;
        }
        private static OracleConnection getOracleConnection(string ConnName = "")
        {

            OracleConnection con = new OracleConnection(Repository.getConnectionString(ConnName));
            return con;
        }

        private static MySqlConnection getMySqlConnection(string ConnName = "")
        {
            //string constr = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
            MySqlConnection con = new MySqlConnection(Repository.getConnectionString(ConnName));
            return con;
        }
        public static DbConnection getConnection(string ConnName = "")
        {
            System.Data.SqlClient.SqlConnectionStringBuilder builder = new System.Data.SqlClient.SqlConnectionStringBuilder(getConnectionString(""));
            dbserver = builder.DataSource;
            dbname = builder.InitialCatalog;

            DbConnection conn = null;
            try
            {
                if (ismssql == 1)
                {
                    conn = Repository.getMSSqlConnection();
                }
                else if (ismssql == 2)
                {
                    conn = Repository.getMySqlConnection();
                }
                else if (ismssql == 3)
                {
                    conn = Repository.getOracleConnection();
                }
                else
                {
                    throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
                }
                conn.Open();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return conn;
        }
        public static string getConnectionString(string Name)
        {
            string constr = string.Empty;
            try
            {
                string connectionName = "DefaultConnection";
                if (getStrLen(Name) > 0)
                {
                    connectionName = Name;
                }
                constr = ConfigurationManager.ConnectionStrings["DefaultConnection"].ToString();
            }
            catch (Exception)
            {
            }
            return constr;
        }

        public static string getUpdateQuery(ArrayList arrlst,string tablename,DbConnection conn,DbTransaction trans)
        {
            string retstr = "";
            string schemastr = string.Empty;

            DataTable dtschema = getTableSchema(tablename, conn, trans);

            if (ismssql == 1)
            {
                retstr = "Alter  " + tablename.ToUpper();
            }
            else if (ismssql == 2)
            {
                retstr = "ALTER TABLE  " + tablename.ToUpper();
                foreach(DataRow dr in dtschema.Rows)
                {

                    if(Convert.ToString(dr["Type"]).Trim().ToUpper().Contains("INT"))
                    {
                        schemastr = Convert.ToString(dr["Field"]).Trim().ToUpper()+ "  INT";
                        if (String.Compare(Convert.ToString(dr["Null"]).Trim().ToUpper(), "YES") == 0)
                        {
                            schemastr += " NULL";
                        }
                        
                    }
                    else if (Convert.ToString(dr["Type"]).Trim().ToUpper().Contains("VARCHAR"))
                    {
                        schemastr = Convert.ToString(dr["Field"]).Trim().ToUpper() + " " + Convert.ToString(dr["Type"]).Trim().ToUpper();
                        if (String.Compare(Convert.ToString(dr["Null"]).Trim().ToUpper(), "YES") == 0)
                        {
                            schemastr += " NULL";
                        }
                    }
                    else
                    {
                        schemastr = Convert.ToString(dr["Field"]).Trim().ToUpper() + " " + Convert.ToString(dr["Type"]).Trim().ToUpper();
                        if (String.Compare(Convert.ToString(dr["Null"]).Trim().ToUpper(), "YES") == 0)
                        {
                            schemastr += " NULL";
                        }
                    }
                    if (schemastr.Length>0)
                    {
                       for(int i=0;i<arrlst.Count;i++)
                        {
                            if(Convert.ToString(arrlst[i]).ToUpper().Contains(schemastr))
                            {
                                arrlst.RemoveAt(i);
                            }
                        }
                    }
                }
            }
            else if (ismssql == 3)
            {
                retstr = "Alter  " + tablename.ToUpper();
            }
            string colstr = string.Empty;
            string[] strarr = null;
            try
            {
                int i = 1;
                foreach (string str in arrlst)
                {
                    strarr = str.Split(' ');
                    if (ismssql == 1)
                    {

                    }
                    else if (ismssql == 2)
                    {
                        //colstr = " "+strarr[0]+" "+ str;
                        colstr = "  MODIFY COLUMN " + str;
                        if (str.ToUpper().Contains("PRIMARY KEY"))
                        {
                            colstr = colstr.ToUpper().Replace("PRIMARY KEY","");
                        }
                    }
                    else if (ismssql == 3)
                    {

                    }
                    else
                    {
                        throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
                    }
                    ///////////////////////////////////////
                    if (arrlst.Count == i)
                    {
                        retstr += colstr;
                    }
                    else
                    {
                        retstr += colstr + ",";
                    }

                    i++;
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
            if(arrlst.Count==0)
            {
                retstr = "";
            }
            return retstr;
        }
        public static string getCreateQuery(ArrayList arrlst, string tablename)
        {
            string retstr = "Create table " + tablename.ToUpper() + "(";

            string[] strarr = null;
            try
            {
                int i = 1;
                foreach (string str in arrlst)
                {
                    strarr = str.Split(' ');
                    if (ismssql == 1)
                    {

                    }
                    else if (ismssql == 2)
                    {

                    }
                    else if (ismssql == 3)
                    {

                    }
                    else
                    {
                        throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
                    }
                    ///////////////////////////////////////
                    if (arrlst.Count == i)
                    {
                        retstr += str;
                    }
                    else
                    {
                        retstr += str + ",";
                    }

                    i++;
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
            retstr += ")";
            return retstr;

        }
        public static bool UpdateTable(ArrayList arrlist, string tablename, DbConnection conn = null, DbTransaction trans = null)
        {
            bool retval = false;
            string query = string.Empty;
            bool isnewtab = false;

            if (ismssql == 1)
            {
                using (SqlCommand cmd = new SqlCommand("select count (*) tname from information_schema.tables  where table_name ='" + tablename.ToUpper() + "'"))
                {
                    cmd.Connection = (SqlConnection)conn;
                    cmd.Transaction = (SqlTransaction)trans;
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        if (reader.HasRows)
                        {
                            if (Convert.ToInt32(reader["tname"]) >0)
                            {
                                isnewtab = false;
                            }
                            else
                            {
                                isnewtab = true;
                            }
                        }
                        else
                        {
                            isnewtab = true;
                        }
                    }
                    //}
                }
                if (isnewtab)
                {
                    query = getCreateQuery(arrlist, tablename);
                }
                else
                {
                    query = getUpdateQuery(arrlist, tablename,conn,trans);
                }
                if (query.Length > 0)
                {
                    using (SqlCommand cmd = new SqlCommand(query, (SqlConnection)conn))
                    {
                        cmd.Transaction = (SqlTransaction)trans;
                        cmd.ExecuteNonQuery();
                    }
                }

            }
            else if (ismssql == 2)
            {
                using (MySqlCommand cmd = new MySqlCommand("SELECT COUNT(*) AS tname FROM information_schema.tables WHERE  table_name ='" + tablename.ToUpper() + "' AND table_schema ='" + dbname + "' LIMIT 1"))
                {
                    cmd.Connection = (MySqlConnection)conn;
                    cmd.Transaction = (MySqlTransaction)trans;
                    using (MySqlDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        if (reader.HasRows)
                        {
                            if (Convert.ToInt32(reader["tname"]) > 0)
                            {
                                isnewtab = false;
                            }
                            else
                            {
                                isnewtab = true;
                            }
                        }
                        else
                        {
                            isnewtab = true;
                        }
                    }
                    //using (OracleDataAdapter sda = new OracleDataAdapter())
                    //{
                    //    sda.SelectCommand = cmd;
                    //using (dt)
                    //{
                    //    sda.Fill(dt);
                    //}
                    //}
                }
                if (isnewtab)
                {
                    query = getCreateQuery(arrlist, tablename);
                }
                else
                {
                    query = getUpdateQuery(arrlist, tablename,conn,trans);
                }
                if (query.Length > 0)
                {
                    using (MySqlCommand cmd = new MySqlCommand(query, (MySqlConnection)conn))
                    {
                        cmd.Transaction = (MySqlTransaction)trans;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            else if (ismssql == 3)
            {
                //using (OracleCommand cmd = new OracleCommand("select tname from tab where tname = '"+tablename.ToUpper()+"';)", (OracleConnection)conn))
                //{
                //    cmd.ExecuteNonQuery();
                //}

                using (OracleCommand cmd = new OracleCommand("select tname from tab where tname = '" + tablename.ToUpper() + "'"))
                {
                    cmd.Connection = (OracleConnection)conn;
                    cmd.Transaction = (OracleTransaction)trans;
                    using (OracleDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        if (reader.HasRows)
                        {
                            if (Convert.ToString(reader["tname"]).Length > 0)
                            {
                                isnewtab = false;
                            }
                            else
                            {
                                isnewtab = true;
                            }
                        }
                        else
                        {
                            isnewtab = true;
                        }
                    }
                    //using (OracleDataAdapter sda = new OracleDataAdapter())
                    //{
                    //    sda.SelectCommand = cmd;
                    //using (dt)
                    //{
                    //    sda.Fill(dt);
                    //}
                    //}
                }
                if (isnewtab)
                {
                    query = getCreateQuery(arrlist, tablename);
                }
                else
                {
                    query = getUpdateQuery(arrlist, tablename,conn,trans);
                }
                if (query.Length > 0)
                {
                    using (OracleCommand cmd = new OracleCommand(query, (OracleConnection)conn))
                    {
                        cmd.ExecuteNonQuery();
                        cmd.Transaction = (OracleTransaction)trans;
                    }
                }

            }


            return retval;
        }

        public static DataTable getTableSchema(string tablename, DbConnection conn, DbTransaction trans)
        {
            DataTable dt = new DataTable();
            if (ismssql == 1)
            {
                using (SqlCommand cmd = new SqlCommand("sp_help " + tablename.ToUpper()))
                {
                    cmd.Connection = (SqlConnection)conn;
                    cmd.Transaction = (SqlTransaction)trans;
                    using (SqlDataAdapter sda = new SqlDataAdapter())
                    {
                        sda.SelectCommand = cmd;
                        using (dt)
                        {
                            sda.Fill(dt);
                        }
                    }
                }

            }
            else if (ismssql == 2)
            {
                using (MySqlCommand cmd = new MySqlCommand("DESCRIBE " + tablename.ToUpper()))
                {
                    cmd.Connection = (MySqlConnection)conn;
                    cmd.Transaction = (MySqlTransaction)trans;
                    using (MySqlDataAdapter sda = new MySqlDataAdapter())
                    {
                        sda.SelectCommand = cmd;
                        using (dt)
                        {
                            sda.Fill(dt);
                        }
                    }

                }
               
            }
            else if (ismssql == 3)
            {
                //using (OracleCommand cmd = new OracleCommand("select tname from tab where tname = '"+tablename.ToUpper()+"';)", (OracleConnection)conn))
                //{
                //    cmd.ExecuteNonQuery();
                //}

                using (OracleCommand cmd = new OracleCommand("DESC " + tablename.ToUpper()))
                {
                    cmd.Connection = (OracleConnection)conn;
                    cmd.Transaction = (OracleTransaction)trans;
                    using (OracleDataAdapter sda = new OracleDataAdapter())
                    {
                        sda.SelectCommand = cmd;
                        using (dt)
                        {
                            sda.Fill(dt);
                        }
                    }

                }

            }
            return dt;
        }


        public static int getStrLen(string str)
        {
            int retval = 0;
            try
            {
                retval = str.Length;
            }
            catch
            {

            }
            return retval;
        }
        public static DataTable getTable(string query, string TableName = "", DbConnection Conn = null, DbTransaction Trans = null, string ConnName = "")
        {
            DataTable dt = new DataTable(TableName);
            try
            {
                if (Conn == null)
                {
                    Conn = Repository.getConnection(ConnName);
                }
                if (ismssql == 1)
                {

                    using (SqlCommand cmd = new SqlCommand(query))
                    {
                        using (Conn)
                        {
                            cmd.Connection = (SqlConnection)Conn;
                            cmd.Transaction = (SqlTransaction)Trans;
                            using (SqlDataAdapter sda = new SqlDataAdapter())
                            {
                                sda.SelectCommand = cmd;
                                using (dt)
                                {
                                    sda.Fill(dt);
                                }
                            }
                        }
                    }
                }
                else if (ismssql == 2)
                {
                    using (MySqlCommand cmd = new MySqlCommand(query))
                    {
                        using (Conn)
                        {
                            cmd.Connection = (MySqlConnection)Conn;
                            cmd.Transaction = (MySqlTransaction)Trans;
                            using (MySqlDataAdapter sda = new MySqlDataAdapter())
                            {
                                sda.SelectCommand = cmd;
                                using (dt)
                                {
                                    sda.Fill(dt);
                                }
                            }
                        }
                    }
                }
                else if (ismssql == 3)
                {
                    using (OracleCommand cmd = new OracleCommand(query))
                    {
                        using (Conn)
                        {
                            cmd.Connection = (OracleConnection)Conn;
                            cmd.Transaction = (OracleTransaction)Trans;
                            using (OracleDataAdapter sda = new OracleDataAdapter())
                            {
                                sda.SelectCommand = cmd;
                                using (dt)
                                {
                                    sda.Fill(dt);
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception)
            {
            }
            finally
            {
                if (Conn != null && Conn.State == ConnectionState.Open)
                {
                    Conn.Close();
                }
            }
            return dt;
        }
        #endregion
        #region Conversion Section
        public static int getInt(string val)
        {
            int retval = 0;
            try
            {
                retval = Convert.ToInt32(val);
            }
            catch (Exception)
            {
            }
            return retval;
        }
        public static double getDouble(string val)
        {
            double retval = 0;
            try
            {
                retval = Convert.ToDouble(val);
            }
            catch (Exception)
            {
            }
            return retval;
        }
        #endregion
    }


}
