using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Repositories
{
    public class Repository
    {
        //public MySqlConnection mysqlcon;
        //public SqlConnection mssqlcon;
        #region Database Section
        public static int ismssql = Convert.ToInt32(ConfigurationManager.AppSettings["DBTYPE"]);//1-MSSQL 2-MYSQL 3-ORACLE
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
            System.Data.SqlClient.SqlConnectionStringBuilder builder = new System.Data.SqlClient.SqlConnectionStringBuilder(getConnectionString(ConnName));
            dbserver = builder.DataSource;
            dbname = builder.InitialCatalog;

            DbConnection conn = null;
            try
            {
                if (ismssql == 1)
                {
                    conn = Repository.getMSSqlConnection(ConnName);
                }
                else if (ismssql == 2)
                {
                    conn = Repository.getMySqlConnection(ConnName);
                }
                else if (ismssql == 3)
                {
                    conn = Repository.getOracleConnection(ConnName);
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
                if (getStrLen(connectionName) > 0)
                {
                    constr = ConfigurationManager.ConnectionStrings[connectionName].ToString();
                }
                else
                {
                    throw new Exception("Error:Please add connection string in web.config.");
                }
            }
            catch (Exception)
            {
            }
            return constr;
        }

        public static string getUpdateQuery(ArrayList arrlst, string tablename, DbConnection conn, DbTransaction trans)
        {
            string retstr = "";
            string schemastr = string.Empty;
            string coltype = string.Empty;
            string nullstr = string.Empty;
            bool isnewcol = true;
            string collstr = string.Empty;
            string colsize = string.Empty;
            Dictionary<string, bool> colltoaddmod = new Dictionary<string, bool>();
            DataTable dtschema = getTableSchema(tablename, conn, trans);

            if (ismssql == 1)
            {

                for (int i = 0; i < arrlst.Count; i++)
                {
                    isnewcol = true;
                    foreach (DataRow dr in dtschema.Rows)
                    {
                        retstr = "";
                        schemastr = Convert.ToString(dr["COLUMN_NAME"]).Trim().ToUpper();
                        //colsize = Convert.ToString(dr["LENGTH"]).Trim().ToUpper();
                        if (Convert.ToInt32(dr["NULLABLE"]) == 1)
                        {
                            nullstr = " NULL";
                        }
                        if (Convert.ToString(dr["TYPE_NAME"]).Trim().ToUpper().Contains("INT"))
                        {
                            coltype = " INT";
                        }
                        else if (Convert.ToString(dr["TYPE_NAME"]).Trim().ToUpper().Contains("VARCHAR"))
                        {
                            coltype = " " + Convert.ToString(dr["TYPE_NAME"]).Trim().ToUpper() + "(" + Convert.ToString(dr["LENGTH"]).Trim() + ")";
                        }
                        else
                        {
                            coltype = " " + Convert.ToString(dr["TYPE_NAME"]).Trim().ToUpper();
                        }
                        if (Convert.ToString(arrlst[i]).ToUpper().Contains("BLOB"))
                        {
                            collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BLOB", "VARBINARY(MAX)");
                            // arrlst[i] = collstr;
                        }
                        else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BOOLEAN"))
                        {
                            collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BOOLEAN", "BIT");
                            //  arrlst[i] = collstr;
                        }
                        else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BOOL"))
                        {
                            collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BOOL", "BIT");
                            // arrlst[i] = collstr;
                        }
                        else if (Convert.ToString(arrlst[i]).ToUpper().Contains("DOUBLE"))
                        {
                            collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("DOUBLE", "BIT");
                            // arrlst[i] = collstr;
                        }
                        else if (Convert.ToString(arrlst[i]).ToUpper().Contains("LONGTEXT"))
                        {
                            collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("LONGTEXT", "VARBINARY(MAX)");
                            // arrlst[i] = collstr;
                        }
                        else
                        {
                            collstr = Convert.ToString(arrlst[i]).ToUpper();
                        }
                        if (schemastr.Length > 0)
                        {

                            if (string.Compare(collstr.Split()[0], schemastr) == 0)
                            {
                                if (!collstr.Contains(schemastr + coltype))
                                {
                                    colltoaddmod.Add(collstr, false);
                                }
                                isnewcol = false;
                                break;
                            }
                        }////
                    }
                    if (isnewcol)
                    {
                        colltoaddmod.Add(collstr, true);
                    }
                }
            }
            else if (ismssql == 2)
            {
                retstr = "ALTER TABLE  " + tablename.ToUpper();
                for (int i = 0; i < arrlst.Count; i++)
                {
                    isnewcol = true;
                    foreach (DataRow dr in dtschema.Rows)
                    {
                        schemastr = Convert.ToString(dr["Field"]).Trim().ToUpper();
                        if (String.Compare(Convert.ToString(dr["Null"]).Trim().ToUpper(), "YES") == 0)
                        {
                            nullstr = " NULL";
                        }
                        if (Convert.ToString(dr["Type"]).Trim().ToUpper().Contains("INT"))
                        {
                            coltype = " INT";
                        }
                        else
                        {
                            coltype = " " + Convert.ToString(dr["Type"]).Trim().ToUpper();
                          
                            if (coltype.ToUpper().Contains("NVARCHAR"))
                            {
                                string tempstr = coltype;
                                tempstr.Replace("NVARCHAR", "VARCHAR");
                                tempstr = tempstr + " CHARSET utf8";
                                coltype = tempstr;
                            }
                        }
                        if (schemastr.Length > 0)
                        {
                            // if (string.Compare(collstr.Split(' ')[0], schemastr) == 0)
                            if (string.Compare(Convert.ToString(arrlst[i]).ToUpper().Split(' ')[0], schemastr) == 0)
                            {
                                if (!Convert.ToString(arrlst[i]).ToUpper().Contains(schemastr + coltype))
                                {
                                    colltoaddmod.Add(Convert.ToString(arrlst[i]).ToUpper(), false);
                                }
                                isnewcol = false;
                                break;
                            }
                        }////
                    }
                    if (isnewcol)
                    {
                        colltoaddmod.Add(Convert.ToString(arrlst[i]).ToUpper(), true);
                    }
                }
            }
            else if (ismssql == 3)
            { //////COLUMN_NAME, DATA_TYPE,DATA_LENGTH,NULLABLE
                retstr = "BEGIN ";
                for (int i = 0; i < arrlst.Count; i++)
                {
                    isnewcol = true;
                    foreach (DataRow dr in dtschema.Rows)
                    {
                        collstr = "";
                        schemastr = Convert.ToString(dr["COLUMN_NAME"]).Trim().ToUpper();
                        //colsize = Convert.ToString(dr["DATA_LENGTH"]).Trim().ToUpper();
                        if (String.Compare(Convert.ToString(dr["NULLABLE"]).Trim().ToUpper(), "Y") == 0)
                        {
                            nullstr = " NULL";
                        }
                        else
                        {
                            nullstr = " NOT NULL";
                        }
                        if (Convert.ToString(dr["DATA_TYPE"]).Trim().ToUpper().Contains("INT"))
                        {
                            coltype = " INT";
                        }
                        else if (Convert.ToString(dr["DATA_TYPE"]).Trim().ToUpper().Contains("NUMBER"))
                        {
                            coltype = " NUMBER" + "(" + Convert.ToString(dr["DATA_LENGTH"]).Trim() + ")";
                        }
                        else if (Convert.ToString(dr["DATA_TYPE"]).Trim().ToUpper().Contains("VARCHAR"))
                        {
                            coltype = " " + Convert.ToString(dr["DATA_TYPE"]).Trim().ToUpper() + "(" + Convert.ToString(dr["DATA_LENGTH"]).Trim() + ")";
                        }
                        else
                        {
                            coltype = " " + Convert.ToString(dr["DATA_TYPE"]).Trim().ToUpper();
                        }
                        if (schemastr.Length > 0)
                        {

                            
                            if (Convert.ToString(arrlst[i]).ToUpper().Contains("DATETIME"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("DATETIME", "TIMESTAMP");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BOOL"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BOOL", "NUMBER(22)");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BOOLEAN"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BOOLEAN", "NUMBER(22)");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BIT"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BIT", "NUMBER(22)");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BINARY"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BINARY", "RAW(2000)");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("DOUBLE"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("DOUBLE", "BINARY_DOUBLE");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("INT"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("INT", "NUMBER(22)");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("VARCHAR"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("VARCHAR", "VARCHAR2");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("LONGTEXT"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("LONGTEXT", "CLOB");
                                // arrlst[i] = collstr;
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("TEXT"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("TEXT", "CLOB");
                            }
                            else
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper();
                            }

                            if (string.Compare(collstr.Split(' ')[0], schemastr) == 0)
                            {
                                if (!collstr.Contains(schemastr + coltype))
                                {
                                    if (collstr.Contains("NOT NULL") && nullstr.Contains("NOT NULL"))
                                    {
                                        collstr = collstr.Replace("NOT NULL", "");
                                    }
                                    else if (collstr.Contains("NULL") && string.Compare(nullstr.Trim(), "NULL") == 0)
                                    {
                                        collstr = collstr.Replace("NULL", "");
                                    }
                                    colltoaddmod.Add(collstr, false);
                                }
                                isnewcol = false;
                                break;
                            }
                        }////
                    }
                    if (isnewcol)
                    {
                        colltoaddmod.Add(collstr, true);
                    }
                }
            }
            string colstr = string.Empty;
            try
            {
                int i = 1;
                foreach (KeyValuePair<string, bool> col in colltoaddmod)
                {
                    if (ismssql == 1)
                    {
                        if (col.Value == true)
                        {
                            colstr = "ALTER TABLE  " + tablename.ToUpper() + "  ADD " + col.Key + ";";
                        }
                        else
                        {
                            if (col.Key.ToUpper().Contains("PRIMARY KEY"))
                            {
                                colstr = col.Key.ToUpper().Replace("PRIMARY KEY", "");
                                colstr = "ALTER TABLE  " + tablename.ToUpper() + "  ALTER COLUMN " + colstr + ";";
                            }
                            else
                            {
                                colstr = "ALTER TABLE  " + tablename.ToUpper() + "  ALTER COLUMN " + col.Key + ";";
                            }
                        }
                        retstr += colstr;
                    }
                    else if (ismssql == 2)
                    {
                        //colstr = " "+strarr[0]+" "+ str;
                        if (col.Value == true)
                        {
                            colstr = "  ADD COLUMN " + col.Key;
                        }
                        else
                        {
                            if (col.Key.ToUpper().Contains("PRIMARY KEY"))
                            {
                                colstr = col.Key.ToUpper().Replace("PRIMARY KEY", "");
                                colstr = "  MODIFY COLUMN " + colstr;
                            }
                            else
                            {
                                colstr = "  MODIFY COLUMN " + col.Key;
                            }
                        }
                        if (colltoaddmod.Count == i)
                        {
                            retstr += colstr;
                        }
                        else
                        {
                            retstr += colstr + ",";
                        }

                    }
                    else if (ismssql == 3)
                    {

                        if (col.Value == true)
                        {
                            colstr = "execute immediate 'ALTER TABLE  " + tablename.ToUpper() + "  ADD " + col.Key + "';";
                        }
                        else
                        {
                            if (col.Key.ToUpper().Contains("PRIMARY KEY"))
                            {
                                colstr = col.Key.ToUpper().Replace("PRIMARY KEY", "");
                                colstr = "execute immediate 'ALTER TABLE  " + tablename.ToUpper() + "  MODIFY " + colstr + "';";
                            }
                            else
                            {
                                colstr = "execute immediate 'ALTER TABLE  " + tablename.ToUpper() + "  MODIFY " + col.Key + "';";
                            }
                        }
                        retstr += colstr;
                    }
                    else
                    {
                        throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
                    }
                    ///////////////////////////////////////

                    i++;
                }
                if (ismssql == 3)
                {
                    retstr += " END;";
                }
            }

            catch (Exception ex)
            {

                throw ex;
            }
            if (colltoaddmod.Count == 0)
            {
                retstr = "";
            }
            return retstr;
        }
        public static void textLog(string strlog)
        {
            try
            {
                //string path = HostingEnvironment.ApplicationHost.GetPhysicalPath();
                string pathfile = System.Web.HttpContext.Current.Server.MapPath("~/log.txt");
                // This text is added only once to the file.
                if (!File.Exists(pathfile))
                {
                    // Create a file to write to.
                    using (StreamWriter sw = File.CreateText(pathfile))
                    {
                        sw.WriteLine(strlog);

                    }
                }
                else
                {
                    using (StreamWriter sw = File.AppendText(pathfile))
                    {
                        sw.WriteLine(strlog);

                    }

                }

                // This text is always added, making the file longer over time
                // if it is not deleted.


            }
            catch (Exception ex)
            {

            }


        }
        public static string getCreateQuery(ArrayList arrlst, string tablename)
        {
            string retstr = "Create table " + tablename.ToUpper() + "(";

            string[] strarr = null;
            string colstr = string.Empty;
            int i = 1;
            try
            {
                if (ismssql == 1)//MSSQL
                {

                    foreach (string str in arrlst)
                    {
                        //strarr = str.Split(' ');
                        if (str.ToUpper().Contains("MEDIUMBLOB"))
                        {
                            colstr = str.ToUpper().Replace("MEDIUMBLOB", "VARBINARY(MAX)");
                        }
                        else if (str.ToUpper().Contains("BLOB"))
                        {
                            colstr = str.ToUpper().Replace("BLOB", "VARBINARY(MAX)");
                        }
                     
                        else if (str.ToUpper().Contains("BOOLEAN"))
                        {
                            colstr = str.ToUpper().Replace("BOOLEAN", "BIT");
                        }
                        else if (str.ToUpper().Contains("BOOL"))
                        {
                            colstr = str.ToUpper().Replace("BOOL", "BIT");
                        }
                        else if (str.ToUpper().Contains("DOUBLE"))
                        {
                            colstr = str.ToUpper().Replace("DOUBLE", "BIT");
                        }
                        else if (str.ToUpper().Contains("LONGTEXT"))
                        {
                            colstr = str.ToUpper().Replace("LONGTEXT", "VARBINARY(MAX)");
                        }
                        else
                        {
                            colstr = str.ToUpper();
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
                else if (ismssql == 2)//mysql
                {
                    foreach (string str in arrlst)
                    {
                        strarr = str.Split(' ');
                       
                        if (strarr[1].ToUpper().Contains("NVARCHAR"))
                        {
                            string tempstr = strarr[1].ToUpper();
                            tempstr.Replace("NVARCHAR", "VARCHAR");
                            tempstr = tempstr + " CHARSET utf8";
                            strarr[1] = tempstr;
                            tempstr = "";
                            for (int j=0;j<strarr.Length;j++)
                            {
                                tempstr += strarr[i];
                            }
                            colstr = tempstr;
                        }
                        else
                        {
                            colstr = str;
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
                else if (ismssql == 3)//Oracle
                {
                    foreach (string str in arrlst)
                    {
                        strarr = str.Split(' ');
                        if (str.ToUpper().Contains("MEDIUMBLOB"))
                        {
                            colstr = str.ToUpper().Replace("MEDIUMBLOB", "BLOB");
                        }
                        else if (str.ToUpper().Contains("DATETIME"))
                        {
                            colstr = str.ToUpper().Replace("DATETIME", "TIMESTAMP");
                        }
                        else if (string.Compare(strarr[1].ToUpper(), "BOOL") == 0)
                        {
                            colstr = str.ToUpper().Replace("BOOL", "NUMBER(22)");
                        }
                        else if (string.Compare(strarr[1].ToUpper(), "BOOLEAN") == 0)
                        {
                            colstr = str.ToUpper().Replace("BOOLEAN", "NUMBER(22)");
                        }
                        else if (string.Compare(strarr[1].ToUpper(), "BIT") == 0)
                        {
                            colstr = str.ToUpper().Replace("BIT", "NUMBER(22)");
                        }
                        else if (string.Compare(strarr[1].ToUpper(), "BINARY") == 0)
                        {
                            colstr = str.ToUpper().Replace("BINARY", "RAW(2000)");
                        }
                        else if (string.Compare(strarr[1].ToUpper(), "DOUBLE") == 0)
                        {
                            colstr = str.ToUpper().Replace("DOUBLE", "BINARY_DOUBLE");
                        }
                        else if (str.ToUpper().Contains("LONGTEXT"))
                        {
                            colstr = str.ToUpper().Replace("LONGTEXT", "CLOB");
                        }
                        else if (str.ToUpper().Contains("TEXT"))
                        {
                            colstr = str.ToUpper().Replace("TEXT", "CLOB");
                        }
                        else
                        {
                            colstr = str.ToUpper();
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
                else
                {
                    throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
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
                    //}
                }
                if (isnewtab)
                {
                    query = getCreateQuery(arrlist, tablename);
                }
                else
                {
                    query = getUpdateQuery(arrlist, tablename, conn, trans);
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
                    query = getUpdateQuery(arrlist, tablename, conn, trans);
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
                    query = getUpdateQuery(arrlist, tablename, conn, trans);
                }
                if (query.Length > 0)
                {
                    using (OracleCommand cmd = new OracleCommand(query, (OracleConnection)conn))
                    {
                        cmd.Transaction = (OracleTransaction)trans;
                        cmd.ExecuteNonQuery();

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
                using (SqlCommand cmd = new SqlCommand("sp_columns " + tablename.ToUpper(), (SqlConnection)conn, (SqlTransaction)trans))
                {
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

                using (OracleCommand cmd = new OracleCommand("Select COLUMN_NAME, DATA_TYPE,DATA_LENGTH,NULLABLE from user_tab_columns where table_name='" + tablename.ToUpper() + "'"))
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
            bool closeconn = false;
            DataTable dt = new DataTable(TableName);
            if (Conn == null)
            {
                Conn = Repository.getConnection(ConnName);
                closeconn = true;
            }
            if (ismssql == 1)
            {

                using (SqlCommand cmd = new SqlCommand(query))
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
            else if (ismssql == 2)
            {
                using (MySqlCommand cmd = new MySqlCommand(query))
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
            else if (ismssql == 3)
            {
                using (OracleCommand cmd = new OracleCommand(query))
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
            if(closeconn)
            {
                if(Conn!=null && ConnectionState.Open==Conn.State)
                {
                    Conn.Close();
                }
            }
            return dt;
        }
        public static DataTable getTableWithLimitOffset(string Tablename, string cols = "", int offset = 0, int limit = 0,string where="", string order_by = "",bool isdesc=false, DbConnection Conn = null, DbTransaction Trans = null, string ConnName = "")
        {
            bool closeconn = false;
            string query = string.Empty;
            string whcl = string.Empty;
            DataTable dt = new DataTable(Tablename);
            if (Conn == null)
            {
                Conn = Repository.getConnection(ConnName);
                closeconn = true;
            }
            if(cols.Length==0)
            {
                cols = "*";
            }
            if(order_by.Length!=0)
            {
                order_by = "order by " + order_by;
                if(isdesc)
                {
                    order_by += " desc";
                }
            }
            if(where.Length!=0)
            {
                whcl = " where " + where;
            }
            if (ismssql == 1)
            {
                query = "Select "+cols+" from "+Tablename+" "+whcl+" "+ order_by + "  OFFSET "+Convert.ToString(offset)+" ROWS FETCH NEXT "+Convert.ToString(limit)+"  ROWS ONLY";
                using (SqlCommand cmd = new SqlCommand(query))
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
            else if (ismssql == 2)
            {
                query = "Select "+cols+" from " + Tablename + " " + whcl + " "+ order_by + " LIMIT " + Convert.ToString(limit) + " OFFSET " + Convert.ToString(offset); ;
                using (MySqlCommand cmd = new MySqlCommand(query))
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
            else if (ismssql == 3)
            {
                query = "SELECT "+cols+" FROM   (SELECT "+cols+" FROM (SELECT "+cols+" FROM DB_POST "+ whcl+ " " + order_by + ") WHERE rownum <= "+Convert.ToString(limit)+") WHERE  rownum >"+Convert.ToString(offset);
                using (OracleCommand cmd = new OracleCommand(query))
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
            if (closeconn)
            {
                if (Conn != null && ConnectionState.Open == Conn.State)
                {
                    Conn.Close();
                }
            }
            return dt;
        }
        public static bool updateTableData(Hashtable ht, string tablename, bool isforupdate, string whereclse = "", DbConnection conn = null, DbTransaction trans = null)
        {
            string retstring = string.Empty;
            bool retval = false;
            string colstr = string.Empty;
            Object colval = null;
            ICollection keys = ht.Keys;
          //  string[] arrwhcl = null;
            if (whereclse.Length > 0)
            {
               // arrwhcl = whereclse.Split('=');
                whereclse = " where " + whereclse;
            }
            // DataTable dt = getTableSchema(tablename, conn, trans);
            int i = 1;
            if (ismssql == 1)
            {
                if (isforupdate)
                {
                    retstring = "Update " + tablename + " set ";
                    foreach (string key in ht.Keys)
                    {
                        if (i == ht.Count)
                        {
                            colstr += key + "=@" + key;
                        }
                        else
                        {
                            colstr += key + "=@" + key + ",";
                        }

                        i++;
                    }
                    retstring = retstring + colstr + " " + whereclse;
                    SqlConnection orconn = (SqlConnection)conn;
                    using (SqlCommand cmd = new SqlCommand(retstring, orconn))
                    {
                        cmd.Transaction = (SqlTransaction)trans;
                        foreach (string key in ht.Keys)
                        {
                            cmd.Parameters.Add(new SqlParameter("@"+key, ht[key]));
                        }
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                            retval = true;
                        }

                    }
                }
                else
                {
                    retstring = "INSERT INTO " + tablename + "(";
                    foreach (string key in ht.Keys)
                    {
                        if (i == ht.Count)
                        {
                            colstr += key;
                            colval += "@" + key;
                        }
                        else
                        {
                            colstr += key + ",";
                            colval += "@" + key + ",";
                        }

                        i++;
                    }
                    retstring += colstr + ") values(" + colval + ")";
 
                    using (SqlCommand cmd = new SqlCommand(retstring))
                    {
                        foreach (string key in ht.Keys)
                        {
                            SqlParameter param = new SqlParameter();
                            param.ParameterName = "@" + key;
                            param.Value = ht[key];
                            cmd.Parameters.Add(param);
                        }
                        // cmd.Parameters.Add("id", DbType.String).Value = empid.Text.Trim();

                        cmd.Connection = (SqlConnection)conn;
                        cmd.Transaction = (SqlTransaction)trans;
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                            retval = true;
                        }

                    }
                }
            }
            else if (ismssql == 2)
            {
                if (isforupdate)
                {
                    retstring = "Update " + tablename + " set ";
                    foreach (string key in ht.Keys)
                    {
                        if (i == ht.Count)
                        {
                            colstr += key + "=@" + key;
                        }
                        else
                        {
                            colstr += key + "=@" + key + ",";
                        }

                        i++;
                    }
                    retstring = retstring + colstr + " " + whereclse;
                    MySqlConnection orconn = (MySqlConnection)conn;
                    using (MySqlCommand cmd = new MySqlCommand(retstring, orconn))
                    {
                        cmd.Transaction = (MySqlTransaction)trans;
                        foreach (string key in ht.Keys)
                        {
                            cmd.Parameters.Add(new MySqlParameter("@"+key, ht[key]));
                        }
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                            retval = true;
                        }

                    }
                }
                else
                {
                    retstring = "INSERT INTO " + tablename + "(";
                    foreach (string key in ht.Keys)
                    {
                        if (i == ht.Count)
                        {
                            colstr += key;
                            colval += "@" + key;
                        }
                        else
                        {
                            colstr += key + ",";
                            colval += "@" + key + ",";
                        }

                        i++;
                    }
                    retstring += colstr + ") values(" + colval + ")";

                    using (MySqlCommand cmd = new MySqlCommand(retstring))
                    {
                        foreach (string key in ht.Keys)
                        {
                            MySqlParameter param = new MySqlParameter();
                            param.ParameterName = "@" + key;
                            param.Value = ht[key];
                            cmd.Parameters.Add(param);
                        }
                        // cmd.Parameters.Add("id", DbType.String).Value = empid.Text.Trim();

                        cmd.Connection = (MySqlConnection)conn;
                        cmd.Transaction = (MySqlTransaction)trans;
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                            retval = true;
                        }

                    }
                }

            }
            else if (ismssql == 3)
            {
                Repository.executeQuery("alter session set nls_date_format = 'DD/MM/YYYY HH24:MI:SS'",conn,trans);
                if (isforupdate)
                {
                    retstring = "Update " + tablename + " set ";
                    foreach (string key in ht.Keys)
                    {
                        if (i == ht.Count)
                        {
                            colstr += key + "=:" + key;
                        }
                        else
                        {
                            colstr += key + "=:" + key + ",";
                        }

                        i++;
                    }
                    retstring = retstring + colstr + " " + whereclse;
                    OracleConnection orconn = (OracleConnection)conn;
                    using (OracleCommand cmd = new OracleCommand(retstring,orconn))
                    {
                        cmd.Transaction = (OracleTransaction)trans;
                        foreach (string key in ht.Keys)
                        {
                            cmd.Parameters.Add(new OracleParameter(key, ht[key]));
                        }
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                            retval = true;
                        }

                    }
                }
                else
                {
                    retstring = "INSERT INTO " + tablename + "(";
                    foreach (string key in ht.Keys)
                    {
                        if (i == ht.Count)
                        {
                            colstr += key;
                            colval += ":" + key;
                        }
                        else
                        {
                            colstr += key + ",";
                            colval += ":" + key + ",";
                        }

                        i++;
                    }
                    retstring += colstr + ") values(" + colval + ")";

                    using (OracleCommand cmd = new OracleCommand(retstring))
                    {
                        foreach (string key in ht.Keys)
                        {
                            //OracleParameter param = new OracleParameter();
                            //param.ParameterName = "@" + key;
                            //param.Value = ht[key];
                            //cmd.Parameters.Add(param);
                            cmd.Parameters.Add(new OracleParameter(key, ht[key]));
                        }
                        // cmd.Parameters.Add("id", DbType.String).Value = empid.Text.Trim();

                        cmd.Connection = (OracleConnection)conn;
                        cmd.Transaction = (OracleTransaction)trans;
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                            retval = true;
                        }

                    }
                }
            }
            else
            {
                throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
            }
            return retval;

        }
        public static DateTime getDateNow()
        {
            DateTime dtime = DateTime.Now;
            CultureInfo provider = CultureInfo.InvariantCulture;
            if (Repository.ismssql==1)
            {
                dtime = Convert.ToDateTime(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", provider));
            }
            else if(Repository.ismssql == 2)
            {
                dtime = Convert.ToDateTime(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", provider)); 
            }
            else if (Repository.ismssql == 3)
            {
                dtime = Convert.ToDateTime(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss", provider));
            }
            
            return dtime;
        }
        public static bool executeQuery(string query, DbConnection conn = null, DbTransaction trans = null)
        {
            bool retval = false;
            bool tocloseconn = false;
            if (conn == null)
            {
                conn = Repository.getConnection();
                tocloseconn = true;
            }
            if (ismssql == 1)
            {
                
                using (SqlCommand cmd = new SqlCommand(query))
                {

                    cmd.Connection = (SqlConnection)conn;
                    cmd.Transaction = (SqlTransaction)trans;
                    int issaved = cmd.ExecuteNonQuery();
                    if (issaved > 0)
                    {
                        retval = true;
                    }

                }
            }
            else if (ismssql == 2)
            {
                using (MySqlCommand cmd = new MySqlCommand(query))
                {
                    // cmd.Parameters.Add("id", DbType.String).Value = empid.Text.Trim();

                    cmd.Connection = (MySqlConnection)conn;
                    cmd.Transaction = (MySqlTransaction)trans;
                    int issaved = cmd.ExecuteNonQuery();
                    if (issaved > 0)
                    {
                        retval = true;
                    }

                }
            }
            else if (ismssql == 3)
            {

                using (OracleCommand cmd = new OracleCommand(query))
                {
                    cmd.Connection = (OracleConnection)conn;
                    cmd.Transaction = (OracleTransaction)trans;
                    int issaved = cmd.ExecuteNonQuery();
                    if (issaved > 0)
                    {
                        retval = true;
                    }
                }
            }
            else
            {
                throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
            }
            if(tocloseconn)
            {
                if(conn!=null && conn.State==ConnectionState.Open)
                {
                    conn.Close();
                }
            }
            return retval;

        }
        public static bool isTableExits(string tablename, DbConnection conn, DbTransaction trans)
        {
            bool retval = false;
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
                            if (Convert.ToInt32(reader["tname"]) > 0)
                            {
                                retval = true;
                            }
                            else
                            {
                                retval = false;
                            }
                        }
                        else
                        {
                            retval = false;
                        }
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
                                retval = true;
                            }
                            else
                            {
                                retval = false;
                            }
                        }
                        else
                        {
                            retval = false;
                        }
                    }
                }
            }
            else if (ismssql == 3)
            {
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
                                retval = true;
                            }
                            else
                            {
                                retval = false;
                            }
                        }
                        else
                        {
                            retval = false;
                        }
                    }
                }
            }
            return retval;
        }
        #endregion
        #region Conversion Section
        public static int getInt(Object val)
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
        public static double getDouble(Object val)
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
        public static string getString(object val)
        {
            string retval = string.Empty;
            try
            {
                retval = Convert.ToString(val).Trim();
            }
            catch (Exception)
            {
            }
            return retval;
        }
        public static bool getBoolean(object val)
        {
            bool retval = false;
            try
            {
                if(val!=DBNull.Value)
                {
                    if(Convert.ToInt32(val)==1)
                    {
                        retval = true;
                    }
                }
            }
            catch (Exception)
            {
            }
            return retval;
        }
        public static DateTime getDate(object val)
        {
            DateTime retval =new DateTime();
            try
            {
                if (val != DBNull.Value)
                {
                      retval =Convert.ToDateTime(val) ;
                    
                }
            }
            catch (Exception)
            {
            }
            return retval;
        }
        public enum Mode
        {
            AlphaNumeric = 1,
            Alpha = 2,
            Numeric = 3
        }
        public static string IncrementInt(string strToIncrement)
        {
            return Repository.getString(Convert.ToInt32(strToIncrement) + 1);
        }
        public static string IncrementStr(string strToIncrement, Mode mode)
        {
            var textArr = strToIncrement.ToCharArray();

            // Add legal characters
            var characters = new List<char>();

            if (mode == Mode.AlphaNumeric || mode == Mode.Numeric)
                for (char c = '0'; c <= '9'; c++)
                    characters.Add(c);

            if (mode == Mode.AlphaNumeric || mode == Mode.Alpha)
                for (char c = 'a'; c <= 'z'; c++)
                    characters.Add(c);

            // Loop from end to beginning
            for (int i = textArr.Length - 1; i >= 0; i--)
            {
                if (textArr[i] == characters.Last())
                {
                    textArr[i] = characters.First();
                }
                else
                {
                    textArr[i] = characters[characters.IndexOf(textArr[i]) + 1];
                    break;
                }
            }

            return new string(textArr);
        }
        public static bool isUnique(string colname, string colval, string table,ref string message, DbConnection conn = null, DbTransaction trans = null)
        {
            bool retval = true;
            DataTable dt = getTable("Select 1 from " + table + " where " + colname + "='" + colval + "'",table,conn,trans);
            if(dt.Rows.Count>0)
            {
                retval = false;
                message = "Already Exist!";
            }
            return retval;
        }

        public static string getMultiString(DataTable dt, string col)
        {
            string retstr = "''";
            if (dt.Rows.Count > 0)
            {
                retstr = string.Empty;
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (i == dt.Rows.Count - 1)
                    {
                        retstr += "'"+ Repository.getString(dt.Rows[i][col])+"'";
                    }
                    else
                    {
                        retstr += "'" + Repository.getString(dt.Rows[i][col]) + "',";
                    }
                }
            }
            return retstr;
        }
        public static List<string> getListStr(DataTable dt, string col)
        {
            List<string> retstr =new List<string>();
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    retstr.Add(Repository.getString(dt.Rows[i][col]));
                }
            }
            return retstr;
        }
        public static string getColIntVal(string colname, string colval, string table, string retcol, DbConnection conn = null, DbTransaction trans = null)
        {
            string retval = string.Empty;
            DataTable dt = getTable("Select " + retcol + " from " + table + " where " + colname + "=" + colval, table, conn, trans);
            if (dt.Rows.Count > 0)
            {
                retval = getString(dt.Rows[0][retcol]);
            }
            return retval;
        }
        public static string getColVal(string colname, string colval, string table, string retcol, DbConnection conn = null, DbTransaction trans = null)
        {
            string retval = string.Empty;
            DataTable dt = getTable("Select "+retcol+" from " + table + " where " + colname + "='" + colval + "'", table, conn, trans);
            if (dt.Rows.Count > 0)
            {
                retval = getString(dt.Rows[0][retcol]);
            }
            return retval;
        }
        public static byte[] getColByteVal(string colname, string colval, string table, string retcol, DbConnection conn = null, DbTransaction trans = null)
        {
            byte[] retval = null;
            DataTable dt = getTable("Select " + retcol + " from " + table + " where " + colname + "='" + colval + "'", table, conn, trans);
            if (dt.Rows.Count > 0)
            {
                if(dt.Rows[0][retcol]!=DBNull.Value)
                { 
                    retval = (byte[])dt.Rows[0][retcol];
                }  
            }
            return retval;
        }
        public static string getMaxVal(string colname,string table, DbConnection conn = null, DbTransaction trans = null)
        {
            string retval = string.Empty;
            DataTable dt = new DataTable();
            if(ismssql==1)
            {
                dt = getTable("Select max(" + colname + ") maxval from " + table, table, conn, trans);
            }
            else if (ismssql == 2)
            {
                dt = getTable("Select max(" + colname + ") maxval from " + table, table, conn, trans);
            }
            else if (ismssql == 3)
            {
                dt = getTable("Select max(" + colname + ") maxval from " + table, table, conn, trans);
            }

            if (dt.Rows.Count > 0)
            {
                retval = getString(dt.Rows[0]["maxval"]);
            }
            return retval;
        }
       
        public static string filterQuertStr(string str)
        {
            string retstr = string.Empty;
            retstr = str.Replace("~~", ".");
            retstr = retstr.Replace("'", "&#39;");
            return retstr;
        }
        public static byte[] ConvertToBytes(HttpPostedFileBase image)
        {
            byte[] imageBytes = null;
            BinaryReader reader = new BinaryReader(image.InputStream);
            imageBytes = reader.ReadBytes((int)image.ContentLength);
            return imageBytes;
        }
        public static int RandomNumber(int min, int max)
        {
            Random random = new Random();
            return random.Next(min, max);
        }
        public static string RandomString(int size, bool lowerCase)
        {
            StringBuilder builder = new StringBuilder();
            Random random = new Random();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                builder.Append(ch);
            }
            if (lowerCase)
                return builder.ToString().ToLower();
            return builder.ToString();
        }
        public static string RandomPassword()
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(RandomString(4, true));
            builder.Append(RandomNumber(1000, 9999));
            builder.Append(RandomString(2, false));
            return builder.ToString();
        }
        
        #endregion

        #region EncryptionDecryption
        public static string Encrypt(string strToEncrypt)
        {
            string EncryptionKey = "WORK2017FOR2018PASSION2019";
            byte[] clearBytes = Encoding.Unicode.GetBytes(strToEncrypt);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateEncryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(clearBytes, 0, clearBytes.Length);
                        cs.Close();
                    }
                    strToEncrypt = Convert.ToBase64String(ms.ToArray());
                }
            }
            return strToEncrypt;
        }

        public static string Decrypt(string strToDecrypt)
        {
            string EncryptionKey = "WORK2017FOR2018PASSION2019";
            byte[] cipherBytes = Convert.FromBase64String(strToDecrypt);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateDecryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(cipherBytes, 0, cipherBytes.Length);
                        cs.Close();
                    }
                    strToDecrypt = Encoding.Unicode.GetString(ms.ToArray());
                }
            }
            return strToDecrypt;
        }
        public static string EncryptKey(string strToEncrypt)
        {
            string EncryptionKey = "WORK2017FOR2018PASSION2030";
            byte[] clearBytes = Encoding.Unicode.GetBytes(strToEncrypt);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateEncryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(clearBytes, 0, clearBytes.Length);
                        cs.Close();
                    }
                    strToEncrypt = Convert.ToBase64String(ms.ToArray());
                }
            }
            return strToEncrypt;
        }

        public static string DecryptKey(string strToDecrypt)
        {
            string EncryptionKey = "WORK2017FOR2018PASSION2030";
            byte[] cipherBytes = Convert.FromBase64String(strToDecrypt);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateDecryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(cipherBytes, 0, cipherBytes.Length);
                        cs.Close();
                    }
                    strToDecrypt = Encoding.Unicode.GetString(ms.ToArray());
                }
            }
            return strToDecrypt;
        }
        public static string GeneratKey(DateTime from,DateTime to)
        {
            string keystring = string.Empty;
            try
            {
                string datefrom = from.ToString("yyyy-MM-dd")+" 00:00";
                string dateto = to.ToString("yyyy-MM-dd")+" 23:59";
                keystring = EncryptKey(datefrom + "~" + to);
            }
            catch (Exception ex)
            {
              //  throw ex;
            }
            return keystring;
        }
        public static bool DeGeneratKey(string keystring,ref DateTime from,ref DateTime to)
        {
            string Decryptstr = "";
            bool retval = false;
            try
            {
                Decryptstr = DecryptKey(keystring);
                string[] arrkeystr = Decryptstr.Split('~');
                from = Convert.ToDateTime(arrkeystr[0]);
                to = Convert.ToDateTime(arrkeystr[1]);
                retval=true;
            }
            catch (Exception ex)
            {
                retval = true;
            }
            return retval;
        }
        #endregion

    }

    public class GeneralizedList<T>
    {
        public static List<T> getTabList(string tablename, string whereclse="",DbConnection conn=null,DbTransaction trans=null)
        {
            if(Repository.getStrLen(whereclse)>0)
            {
                whereclse = " where " + whereclse;
            }
            DataTable dt = Repository.getTable("Select * from "+tablename+" "+ whereclse, tablename,conn,trans);
            List<T> lst = new List<T>();
            T clsobj = default(T);
            foreach (DataRow d in dt.Rows)
            {
                clsobj = (T)Activator.CreateInstance(typeof(T));
                
                foreach (var prop in clsobj.GetType().GetProperties())
                {
                    prop.SetValue(clsobj, d[prop.Name]);
                }
                lst.Add(clsobj);
            }

            return lst;
        }
        public static T getTabRowData(string tablename, string whereclse = "", DbConnection conn = null, DbTransaction trans = null)
        {
            if (Repository.getStrLen(whereclse) > 0)
            {
                whereclse = " where " + whereclse;
            }
            DataTable dt = Repository.getTable("Select * from " + tablename + " " + whereclse, tablename, conn, trans);
            T clsobj = clsobj = (T)Activator.CreateInstance(typeof(T));
            if(dt.Rows.Count>0)
            { 
                foreach (var prop in clsobj.GetType().GetProperties())
                {
                    prop.SetValue(clsobj, dt.Rows[0][prop.Name]);
                }
            }
            return clsobj;
        }
        public static void UpdateTable(T obj,string tablename,string whereclse="",bool isUpdate=false,DbConnection conn=null,DbTransaction trans=null)
        {
            Hashtable ht = new Hashtable();
            foreach (var prop in obj.GetType().GetProperties())
            {
                ht.Add(prop.Name, prop.GetValue(obj, prop.GetIndexParameters()));
            }
            Repository.updateTableData(ht, tablename, isUpdate, whereclse, conn, trans);
        }


    }


}
