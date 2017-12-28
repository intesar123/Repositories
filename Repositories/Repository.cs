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
                            collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BLOB", "BINARY");
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
                        colltoaddmod.Add(Convert.ToString(arrlst[i]).ToUpper(), true);
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

                            if (Convert.ToString(arrlst[i]).ToUpper().Contains("TEXT"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("TEXT", "CLOB");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("DATETIME"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("DATETIME", "DATE");
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
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("BINARY"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("BINARY", "RAW(2000)");
                            }
                            else if (Convert.ToString(arrlst[i]).ToUpper().Contains("VARCHAR"))
                            {
                                collstr = Convert.ToString(arrlst[i]).ToUpper().Replace("VARCHAR", "VARCHAR2");
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
                        colltoaddmod.Add(Convert.ToString(arrlst[i]).ToUpper(), true);
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
                        if (str.ToUpper().Contains("BLOB"))
                        {
                            colstr = str.ToUpper().Replace("BLOB", "BINARY");
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
                else if (ismssql == 3)//Oracle
                {
                    foreach (string str in arrlst)
                    {
                        strarr = str.Split(' ');
                        if (str.ToUpper().Contains("TEXT"))
                        {
                            colstr = str.ToUpper().Replace("TEXT", "CLOB");
                        }
                        else if (str.ToUpper().Contains("DATETIME"))
                        {
                            colstr = str.ToUpper().Replace("DATETIME", "DATE");
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
            DataTable dt = new DataTable(TableName);
            if (Conn == null)
            {
                Conn = Repository.getConnection(ConnName);
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

            return dt;
        }
        public static bool updateTableData(Hashtable ht, string tablename, bool isforupdate, DbConnection conn, DbTransaction trans)
        {
            string retstring = string.Empty;
            bool retval = false;
            string colstr = string.Empty;
            Object colval = null;
            ICollection keys = ht.Keys;
            // DataTable dt = getTableSchema(tablename, conn, trans);
            int i = 1;
            if (ismssql == 1)
            {
                if (isforupdate)
                {

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
                        foreach(string key in ht.Keys)
                        {
                            SqlParameter param = new SqlParameter();
                            param.ParameterName = "@"+key;
                            param.Value = ht[key];
                            cmd.Parameters.Add(param);
                        }
                       // cmd.Parameters.Add("id", DbType.String).Value = empid.Text.Trim();
                      
                        cmd.Connection =(SqlConnection) conn;
                        cmd.Transaction = (SqlTransaction)trans;
                        int issaved = cmd.ExecuteNonQuery();
                        if (issaved > 0)
                        {
                           retval=true;
                        }

                    }
                }
            }
            else if(ismssql==2)
            {
                if (isforupdate)
                {

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
            else if(ismssql==3)
            {

                if (isforupdate)
                {

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
                            cmd.Parameters.Add(new OracleParameter(key,ht[key]));
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
        public static bool executeQuery(string query,DbConnection conn=null,DbTransaction trans=null)
        {
            bool retval = false;

            return retval;

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
        #endregion

        #region Application
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

    }


}
