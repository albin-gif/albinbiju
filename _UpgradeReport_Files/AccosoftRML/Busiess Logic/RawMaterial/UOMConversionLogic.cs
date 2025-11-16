using System;
using System.Data; using AccosoftUtilities;   using AccosoftLogWriter;  using AccosoftNineteenCDL;   
using System.Configuration;
using System.Linq;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections; 
using System.Collections.Generic;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class UOMConversionLogic
    {
          static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        #region "Method Queries"

        public UOMConversionLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public DataTable FillUOMCode(string col , string TypeID)// Fill LookUps
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                if (col == "0")
                {
                    SQL = " SELECT   RM_RMM_RM_CODE CODE, RM_RMM_RM_DESCRIPTION NAME ";
                    SQL = SQL + "     FROM rm_rawmaterial_master";
                    SQL = SQL + " ORDER BY RM_RMM_RM_CODE";

                }
                else if (col == "2")
                {
                    SQL = " SELECT RM_UM_UOM_CODE CODE, RM_UM_UOM_DESC NAME";
                    SQL = SQL + "   FROM RM_UOM_MASTER ";
                }
                else if (col == "4")
                {
                    SQL = " SELECT RM_UM_UOM_CODE CODE, RM_UM_UOM_DESC NAME";
                    SQL = SQL + "   FROM RM_UOM_MASTER ";
                }
               
                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;

        }

        public DataSet FetchUOMMissingData()
        {
            DataSet dtReturn = new DataSet();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                DataTable dtUOMMissing = new DataTable();
                DataTable dtConverMaster = new DataTable();

                  
  
                SQL = " SELECT   RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION, BASEUOMCODE, BASEUOMNAME, ";
                SQL = SQL + "        MIXUOMCODE, MIXUOMNAME, CONVFACT, RM_UM_UOM_CODE_CONS, ";
                SQL = SQL + "        RM_MASTER_UOM_CONS_NAME ";
                SQL = SQL + "    FROM RM_UOM_CONVERSION_RFSH_VW ";
                SQL = SQL + "   ORDER BY RM_RMM_RM_CODE ASC ";

                dtUOMMissing = clsSQLHelper.GetDataTableByCommand(SQL);
                dtReturn.Tables.Add(dtUOMMissing);
               

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                ocConn.Close();
                ocConn.Dispose();
            }
            return dtReturn;

        }

        public DataSet FetchUOMConversionData()//Fill BankData
        {
            DataSet dtReturn = new DataSet();

            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = "  ";
                SQL = SQL + "SELECT RM_UOM_CONVERSION.RM_UCM_SL_NO, ";
                SQL = SQL + " RM_UOM_CONVERSION.RM_UM_UOM_CODE_FROM, RM_UOM_MASTER.RM_UM_UOM_DESC FROMUOM, ";
                SQL = SQL + "  RM_UOM_CONVERSION.RM_RMM_RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,  ";
                SQL = SQL + "  RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO, TOUOM.RM_UM_UOM_DESC TOUOM,  ";
                SQL = SQL + "  RM_UOM_CONVERSION.RM_UCM_FACTOR ";
                SQL = SQL + "FROM RM_UOM_CONVERSION , RM_UOM_MASTER , RM_RAWMATERIAL_MASTER , RM_UOM_MASTER TOUOM ";
                SQL = SQL + "WHERE (RM_UOM_CONVERSION.RM_UM_UOM_CODE_FROM = RM_UOM_MASTER.RM_UM_UOM_CODE) ";
                SQL = SQL + "AND(RM_UOM_CONVERSION.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE) ";
                SQL = SQL + "AND(RM_UOM_CONVERSION.RM_UM_UOM_CODE_TO = TOUOM.RM_UM_UOM_CODE) ";
                SQL = SQL + " ORDER BY RM_UOM_CONVERSION.RM_RMM_RM_CODE ASC ";

                dtReturn = clsSQLHelper.GetDataset(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                ocConn.Close();
                ocConn.Dispose();
            }
            return dtReturn;

        }

        //public DataSet RefreshData()//Fill BankData
        //{
        //    DataSet dtReturn = new DataSet();

        //    try
        //    {

        //        SQL = string.Empty;
        //        SqlHelper clsSQLHelper = new SqlHelper();
        //        DataTable dtUOM = new DataTable  (); 
        //        DataTable dtConverMaster = new DataTable () ; 

        //        SQL = "   SELECT DISTINCT rm_rawmaterial_master.RM_RMM_RM_CODE , ";
        //        SQL = SQL  + "        rm_rawmaterial_master.RM_RMM_RM_DESCRIPTION, ";
        //        SQL = SQL  + "        rm_rawmaterial_master.RM_UM_UOM_CODE BASEUOMCODE , ";
        //        SQL = SQL  + "        RM_UOM_MASTER.RM_UM_UOM_DESC BASEUOMNAME, ";
        //        SQL = SQL  + "        case  when  TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE  is null  then  ";
        //        SQL = SQL  + "        rm_rawmaterial_master.RM_UM_UOM_CODE_CONS   ";
        //        SQL = SQL  + "        else  ";
        //        SQL = SQL  + "        TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE ";
        //        SQL = SQL  + "        end as  ";
        //        SQL = SQL  + "       MIXUOMCODE, ";
        //        SQL = SQL  + "        case when   UOMCOMPOSITION.RM_UM_UOM_DESC  is null then  ";
        //        SQL = SQL  + "        RM_CONSUMPTION.RM_UM_UOM_DESC   ";
        //        SQL = SQL  + "        else  ";
        //        SQL = SQL  + "        UOMCOMPOSITION.RM_UM_UOM_DESC ";
        //        SQL = SQL  + "        end  as  ";
        //        SQL = SQL  + "        MIXUOMNAME, 1 CONVFACT ,  ";
        //        SQL = SQL  + "            rm_rawmaterial_master.RM_UM_UOM_CODE_CONS  ,    RM_CONSUMPTION.RM_UM_UOM_DESC RM_MASTER_UOM_CONS_NAME ";
        //        SQL = SQL  + "   FROM rm_rawmaterial_master, ";
        //        SQL = SQL  + "        TECH_PRODUCT_COMP_DETAILS, ";
        //        SQL = SQL  + "        RM_UOM_MASTER, ";
        //        SQL = SQL  + "        RM_UOM_MASTER UOMCOMPOSITION , ";
        //        SQL = SQL  + "        RM_UOM_MASTER RM_CONSUMPTION ";
        //        SQL = SQL  + "  WHERE  ";
        //        SQL = SQL  + "   rm_rawmaterial_master.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
        //        SQL = SQL  + "   AND   rm_rawmaterial_master.RM_UM_UOM_CODE_CONS =     RM_CONSUMPTION.RM_UM_UOM_CODE ";
        //        SQL = SQL  + "         AND  TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE =    UOMCOMPOSITION.RM_UM_UOM_CODE   (+)    ";
        //        SQL = SQL  + "         AND  rM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  =  TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE (+) ";
        //        SQL = SQL + "   ORDER BY rm_rawmaterial_master.RM_RMM_RM_CODE ";
        //        dtUOM = clsSQLHelper.GetDataTableByCommand(SQL);

        //        dtReturn.Tables.Add(dtUOM); 

        //    SQL = " SELECT  " ;
        //    SQL = SQL +  "    RM_UCM_SL_NO, RM_RMM_RM_CODE, RM_UM_UOM_CODE_FROM,  ";
        //    SQL = SQL +  "    RM_UM_UOM_CODE_TO, RM_UCM_FACTOR, AD_CM_COMPANY_CODE,  ";
        //    SQL = SQL +  "    RM_UCM_FROM_DATE, RM_UCM_TO_DATE, RM_UCM_DELETESTATUS,  ";
        //    SQL = SQL +  "    RM_UCM_CONVERSION_ID ";
        //    SQL = SQL + "    FROM  RM_UOM_CONVERSION  ";

        //    dtConverMaster = clsSQLHelper.GetDataTableByCommand(SQL);

        //    dtReturn.Tables.Add(dtConverMaster); 




        //    }
        //    catch (Exception ex)
        //    {
        //        ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
        //        objLogWriter.WriteLog(ex);
        //    }
        //    finally
        //    {
        //        ocConn.Close();
        //        ocConn.Dispose();
        //    }
        //    return dtReturn;

        //}

        #endregion

        #region "DML"

        public string InsertSql(List<UOMMissingDetails> EntityUOMDet,object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                // object oCode;
               OracleHelper oTrns = new OracleHelper();
               SessionManager mngrclass = (SessionManager)mngrclassobj;
                DataSet dsMaxConversionID = new DataSet();
                DataTable dtSLNO = new DataTable();
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = string.Empty;

                int livar = 0;
                 //object lobjFromDate = null;
                 //object lobjToDate = null;

               Int16 miDeleteStatus = 0;

                SQL = " SELECT  NVL(MAX(RM_UCM_SL_NO),0) SL_NO FROM RM_UOM_CONVERSION  ";
                dtSLNO = clsSQLHelper.GetDataTableByCommand(SQL);
                livar = Convert.ToInt32(dtSLNO.Rows[0]["SL_NO"]);
                SQL = string.Empty;

                //SQL = "DELETE FROM rm_uom_conversion ";
                //     // WHERE AD_CM_COMPANY_CODE='"  + msCompanyCode  + "'"
                // sSQLArray.Add(SQL);

                foreach (var Data in EntityUOMDet)
                {
                    if(livar==0)
                    {
                        livar = 0;
                    }
                    else
                    {
                        ++livar;
                    }
                   
                    SQL = "INSERT INTO rm_uom_conversion (RM_UCM_SL_NO, RM_UM_UOM_CODE_FROM, RM_UM_UOM_CODE_TO, ";
                    SQL = SQL + "AD_CM_COMPANY_CODE, RM_UCM_FACTOR, RM_UCM_DELETESTATUS, RM_RMM_RM_CODE, ";
                    SQL = SQL + "RM_UCM_FROM_DATE, RM_UCM_TO_DATE, RM_UCM_CONVERSION_ID) VALUES ";
                    SQL = SQL + "(" + livar + ",'" + Data.lobjFromUnit + "', '" + Data.lobjToUnit + "',";
                    SQL = SQL + "'" + mngrclass.CompanyCode + "'," + Data.lobjFacto + "," + miDeleteStatus + ",";
                    SQL = SQL + "'" + Data.lobjRMCode + "','',";
                    SQL = SQL + "'',1)";

                    sSQLArray.Add(SQL);
                }
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMUCM", "1", false, Environment.MachineName, "I", sSQLArray);
                
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

        public string UpdateConversion(string RMCode, object mngrclassobj, string RMName, string FromUOM, string FromUOMName, string ToUoM, string ToUOMName,
            double ConFactor, string SLNO)
        {

            string sRetun = string.Empty;
            OracleHelper oTrns = new OracleHelper();
            SessionManager mngrclass = (SessionManager)mngrclassobj;
            sSQLArray.Clear();
            try
            {

                SQL = "  UPDATE RM_UOM_CONVERSION  ";
                SQL = SQL + " SET     RM_UM_UOM_CODE_FROM  = '" + FromUOM + "',  ";
                SQL = SQL + "         RM_UM_UOM_CODE_TO    = '" + ToUoM + "',  ";
                SQL = SQL + "         RM_UCM_FACTOR        = " + ConFactor + "";
                SQL = SQL + "  WHERE  RM_RMM_RM_CODE       = '" + RMCode + "'  ";
                SQL = SQL + "  AND    RM_UCM_SL_NO  = " + SLNO + "  ";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMUCM", RMCode, false, Environment.MachineName, "U", sSQLArray);


            }

            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);

                return sRetun;
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


    public class UOMMissingDetails
    {
        public object lobjFromUnit { get; set; }
        public object lobjToUnit { get; set; }
        public double lobjFacto { get; set; }
        public object lobjRMCode { get; set; }
    }

}
