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
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class GPEMaterialPlantMappingLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        #region "DetchData"

        public DataSet GetPlant ( )
        {

            DataSet sReturn = new DataSet();
            DataTable dtPlant = new DataTable();
            DataTable dtSource = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  TECH_PLM_PLANT_CODE PLANT_CODE, ";
                SQL = SQL + " TECH_PLM_PLANT_NAME PLANT_NAME  ";
                SQL = SQL + " FROM TECH_PLANT_MASTER ";
                SQL = SQL + " WHERE TECH_PLM_GPE_INT_YN ='Y'  ";
                SQL = SQL + " order By  TECH_PLM_PLANT_CODE  ";

                dtPlant = clsSQLHelper.GetDataTableByCommand(SQL);
                dtPlant.TableName = "Plant";
                sReturn.Tables.Add(dtPlant);

                SQL = " SELECT  RM_RMM_RM_CODE RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION RM_NAME  ";
                SQL = SQL + " FROM RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + "  WHERE   RM_RMM_RM_CODE   IN(     ";
                SQL = SQL + "  select   DISTINCT   RM_RMM_RM_CODE  FROM TECH_PRODUCT_COMP_DETAILS   )  ";
 
                SQL = SQL + " order By  RM_RMM_RM_CODE  ";

                dtSource = clsSQLHelper.GetDataTableByCommand(SQL);
                dtSource.TableName = "RawMaterial";
                sReturn.Tables.Add(dtSource);



            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }

            return sReturn;
        }

        public DataSet FetchDataTripRate ( )
        {

            DataSet sReturn = new DataSet();
            DataTable dtPlant = new DataTable();
            DataTable dtSource = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT  RM_GPE_MAPPING_MASTER.RM_RMM_RM_CODE RM_CODE, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME , ";
                SQL = SQL + " TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE PLANT_CODE,  ";
                SQL = SQL + " TECH_PLM_PLANT_NAME PLANT_NAME , ";
                SQL = SQL + " GPE_RM_MAP_CODE  ";
                SQL = SQL + " FROM RM_GPE_MAPPING_MASTER,RM_RAWMATERIAL_MASTER,TECH_PLANT_MASTER  ";
                SQL = SQL + " where RM_GPE_MAPPING_MASTER.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + " AND RM_GPE_MAPPING_MASTER.TECH_PLM_PLANT_CODE=TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE  ";

                sReturn = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }

            return sReturn;
        }

            #region "DML"


        public string UpdateSQL(List<GPEMaterialPlantMappingEntity> GridEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                // object oCode;
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                DataSet dsMaxConversionID = new DataSet();
                

                SQL = "DELETE FROM RM_GPE_MAPPING_MASTER ";
         
                sSQLArray.Add(SQL);

                foreach (var Data in GridEntity)
                {
                   
                    SQL = "INSERT INTO RM_GPE_MAPPING_MASTER (RM_RMM_RM_CODE , TECH_PLM_PLANT_CODE , GPE_RM_MAP_CODE  ";
                    SQL = SQL + " ) VALUES ";
                    SQL = SQL + "('" + Data.ERmCode + "', '" + Data.EPlantCode + "',";
                 //   SQL = SQL + " " + Convert.ToDouble(Data.ETripRate) + " ";
                    SQL = SQL + " '" + Data.ERmMapCode + "' ";
                    SQL = SQL + ")";

                    sSQLArray.Add(SQL);
                }
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMGPEPLMAP", "1", false, Environment.MachineName, "I", sSQLArray);

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

        #endregion 
    }
}


   #region "Entity"

    public class GPEMaterialPlantMappingEntity
    {

        public string EPlantCode
        {
            get;
            set;

        }
        public string ERmCode
        {
            get;
            set;

        }
        public string ERmMapCode
        {
            get;
            set;

        }
    }
    
     #endregion
