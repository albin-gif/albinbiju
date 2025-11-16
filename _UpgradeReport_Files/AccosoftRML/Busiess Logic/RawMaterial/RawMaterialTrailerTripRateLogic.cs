using System;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class RawMaterialTrailerTripRateLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;


        #region "FetchData"

        public DataSet GetPlant()
        {

               DataSet sReturn = new DataSet();
               DataTable dtPlant = new DataTable();
               DataTable dtSource = new DataTable();

                try
                {
                    SQL = string.Empty;
                    SqlHelper clsSQLHelper = new SqlHelper();

                    SQL = " SELECT  TECH_PLM_PLANT_CODE PLANT_CODE,";
                    SQL = SQL + " TECH_PLM_PLANT_NAME PLANT_NAME ";
                    SQL = SQL + " FROM TECH_PLANT_MASTER ";
                    SQL = SQL + " order By  TECH_PLM_PLANT_CODE ";

                    dtPlant = clsSQLHelper.GetDataTableByCommand(SQL);
                    dtPlant.TableName = "Plant";
                    sReturn.Tables.Add(dtPlant);

                    SQL = " SELECT  RM_SM_SOURCE_CODE SOURCE_CODE,";
                    SQL = SQL + " RM_SM_SOURCE_DESC SOURCE_NAME ";
                    SQL = SQL + " FROM RM_SOURCE_MASTER ";
                    SQL = SQL + " order By  RM_SM_SOURCE_CODE ";

                    dtSource = clsSQLHelper.GetDataTableByCommand(SQL);
                    dtSource.TableName = "Source";
                    sReturn.Tables.Add(dtSource);



                }
                catch (Exception ex)
                {
                    ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                    objLogWriter.WriteLog(ex);
                    
                }

                return sReturn;
         }

        public DataSet FetchDataTripRate()
        {

            DataSet sReturn = new DataSet();
            DataTable dtPlant = new DataTable();
            DataTable dtSource = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT  RM_DEFUALTS_TRAILER_TRIP_RATE.RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME ,";
                SQL = SQL + " TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE PLANT_CODE, ";
                SQL = SQL + " TECH_PLM_PLANT_NAME PLANT_NAME ,";
                SQL = SQL + " RM_DFT_TRIP_RATE ";
                SQL = SQL + " FROM RM_DEFUALTS_TRAILER_TRIP_RATE,RM_SOURCE_MASTER,TECH_PLANT_MASTER ";
                SQL = SQL + " where RM_DEFUALTS_TRAILER_TRIP_RATE.RM_SM_SOURCE_CODE=RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + " AND RM_DEFUALTS_TRAILER_TRIP_RATE.TECH_PLM_PLANT_CODE=TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE ";

                sReturn = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }

            return sReturn;
        }

        #endregion


        #region "DML"


        public string UpdateSQL(List<RMTrailerTripRateEntity> GridEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                // object oCode;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                DataSet dsMaxConversionID = new DataSet();
                

                SQL = "DELETE FROM RM_DEFUALTS_TRAILER_TRIP_RATE ";
         
                sSQLArray.Add(SQL);

                foreach (var Data in GridEntity)
                {
                   
                    SQL = "INSERT INTO RM_DEFUALTS_TRAILER_TRIP_RATE (RM_SM_SOURCE_CODE , TECH_PLM_PLANT_CODE , RM_DFT_TRIP_RATE  ";
                    SQL = SQL + " ) VALUES ";
                    SQL = SQL + "('" + Data.ESourceCode + "', '" + Data.EPlantCode + "',";
                    SQL = SQL + " " + Convert.ToDouble(Data.ETripRate) + " ";
                    SQL = SQL + ")";

                    sSQLArray.Add(SQL);
                }
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTTRIPRATE", "1", false, Environment.MachineName, "I", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;

        }

        #endregion
           

    }

       #region "Entity"

    public class RMTrailerTripRateEntity
    {

        public string EPlantCode
        {
            get;
            set;

        }
        public string ESourceCode
        {
            get;
            set;

        }
        public double ETripRate
        {
            get;
            set;

        }
    }
    
     #endregion
}
