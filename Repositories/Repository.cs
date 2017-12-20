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
            string constr = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
            MySqlConnection con = new MySqlConnection(Repository.getConnectionString(ConnName));
            return con;
        }
        public static DbConnection getConnection(string ConnName)
        {
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
            }
            catch (Exception)
            {
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

        public static string getUpdateQuery(ArrayList arrlst)
        {
            string retstr = string.Empty;
            try
            {
                foreach(string str in arrlst)
                {
                    if(ismssql==1)
                    {
                         
                    }
                    else if(ismssql==2)
                    {

                    }
                    else if(ismssql == 3)
                    {

                    }
                    else
                    {
                        throw new Exception("Please set value for the key DBTYPE in web.config appsetting section");
                    }
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
            return retstr;

        }
        public static string getCreateQuery(ArrayList arrlst)
        {
            string retstr = string.Empty;
            try
            {
                foreach (string str in arrlst)
                {
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
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
            return retstr;

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
