using System;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Web.UI.WebControls;
using System.Collections.Generic;

/// <summary>
/// Summary description for ItemMasterLogic
/// </summary>
/// 
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class ItemMasterLogic
    {

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());



        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public ItemMasterLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }



        #region "keypress Enter"

        public String FetchAccountGridKeypressCode ( string sSelected )
        {
            string sRetName = "";

            DataTable dtType = new DataTable();

            try
            {

                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT GL_COAM_ACCOUNT_CODE CODE, GL_COAM_ACCOUNT_NAME NAME ,GL_COAM_ACCOUNT_GROUP  FIELDTHREE ";
                SQL = SQL + "  FROM GL_COA_MASTER , RM_DEFUALTS_PARAMETERS_LOOK_UP WHERE   ";
                SQL = SQL + "  GL_COAM_LEDGER_TYPE = 'SUB' AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";
                SQL = SQL + " AND  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE= RM_DEFUALTS_PARAMETERS_LOOK_UP. PARAMETER_VALUE ";

                SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_CONSUMPTION_ACC_CODE' ";

                SQL = SQL + " 	  AND  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE  ='" + sSelected + "'";



                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtType.Rows.Count > 0)
                {
                    sRetName = dtType.Rows[0]["NAME"].ToString();
                }

                else
                {
                    sRetName = "Incorrect Account Code.Kindly Enter Correct Code";
                }
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();

                objLogWriter.WriteLog(ex);

                sRetName = "Error";
            }

            finally
            {
                //ocConn.Close();

                //ocConn.Dispose();
            }

            return sRetName;
        }
        #endregion
        #region "FEtch Data" 


        public DataTable FetchData ( string sCode )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the company type master recrods 

                SQL = " SELECT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION, RM_RMM_RM_SHORT_NAME , ";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_CM_CATEGORY_CODE, RM_CATEGORY_MASTER.RM_CM_CATEGORY_DESC, ";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE, RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_DESC,RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCAT_CODE_PATTERN_YN, ";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC, RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE, ";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_RM_STATUS, ";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_INV_ACC_CODE,INV.GL_COAM_ACCOUNT_NAME INVENTORY_ACC_NAME, ";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_COS_ACC_CODE,COSTOFSALES.GL_COAM_ACCOUNT_NAME COS_ACCOUNT_NAME, ";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_CONS_ACC_CODE,CONS.GL_COAM_ACCOUNT_NAME CONS_ACCOUNT_NAME, ";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_INC_ACC_CODE,INCOME.GL_COAM_ACCOUNT_NAME INCOME_ACCOUNT_NAME,  ";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_INV_TRANS_ACC_CODE,TRANS.GL_COAM_ACCOUNT_NAME TRANS_ACCOUNT_NAME,";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_FINISHED_GOODS_ACC_CODE,FINISHDGDS.GL_COAM_ACCOUNT_NAME FINISHEDGD_NAME, ";

                SQL = SQL + "RM_RAWMATERIAL_MASTER.RM_RMM_PRODUCT_TYPE, RM_RMM_VAT_APPLICABLE_YN, RM_RMM_PHYSICAL_STOCK_YN ,";

                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_REMARKS,RM_RAWMATERIAL_MASTER.RM_RMM_MAT_DENSITY, RM_UM_UOM_CODE_CONS,RM_UM_UOM_OTHER_ITEM_CONS,RM_RMM_OTHER_ITEM_CONFACT,";//to added RM_RMM_MAT_DENSITY

                SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,RM_RMM_OLD_REF_NO,RM_RMM_RM_DIAMETER, PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME,RM_RMM_BBS_REF_CODE,RM_RMM_RECEIPT_QTY_VARIENCE,RM_RMM_ALLOW_MINUS_QTY_YN  ";

                SQL = SQL + " FROM RM_CATEGORY_MASTER , RM_CATEGORY_SUB_MASTER , RM_UOM_MASTER , RM_RAWMATERIAL_MASTER , ";
                SQL = SQL + " 	GL_COA_MASTER INV,";
                SQL = SQL + " 	GL_COA_MASTER COSTOFSALES,";
                SQL = SQL + " 	GL_COA_MASTER CONS,";
                SQL = SQL + "   GL_COA_MASTER INCOME,";
                SQL = SQL + "   GL_COA_MASTER TRANS, ";
                SQL = SQL + "   GL_COA_MASTER FINISHDGDS, ";
                SQL = SQL + "   PC_BUD_RESOURCE_MASTER ";

                SQL = SQL + " WHERE  RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE = RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE (+) ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_CM_CATEGORY_CODE = RM_CATEGORY_MASTER.RM_CM_CATEGORY_CODE (+)";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE (+)";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.RM_RMM_INV_ACC_CODE =INV.GL_COAM_ACCOUNT_CODE  (+)";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.RM_RMM_COS_ACC_CODE =COSTOFSALES.GL_COAM_ACCOUNT_CODE  (+)";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.RM_RMM_CONS_ACC_CODE =CONS.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.RM_RMM_INC_ACC_CODE =INCOME.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.RM_RMM_INV_TRANS_ACC_CODE =TRANS.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_FINISHED_GOODS_ACC_CODE=FINISHDGDS.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE= PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+) ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ='" + sCode + "'";

                SQL = SQL + " ORDER BY  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC ";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);
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
            return dtReturn;

        }

        public DataTable FetchStLinkDetails ( string RM_RMM_RM_CODE )
        {
            DataTable dtSTLinkDetails = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "select  RM_RAWMATERIAL_BATCH_LINK_DLTS.TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME,";
                SQL = SQL + " RM_RAWMATERIAL_BATCH_LINK_DLTS.SALES_STN_STATION_CODE,SALES_STN_STATION_NAME,RM_CODE_BATCHING_PLANT , RM_UM_UOM_CODE_PLANT ,   RM_UOM_MASTER.RM_UM_UOM_DESC  ";
                SQL = SQL + " from RM_RAWMATERIAL_BATCH_LINK_DLTS,SL_STATION_MASTER,TECH_PLANT_MASTER , RM_UOM_MASTER  ";
                SQL = SQL + " where RM_RMM_RM_CODE='" + RM_RMM_RM_CODE + "' and RM_RAWMATERIAL_BATCH_LINK_DLTS.TECH_PLM_PLANT_CODE=TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE";
                SQL = SQL + " and RM_RAWMATERIAL_BATCH_LINK_DLTS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + " and   RM_RAWMATERIAL_BATCH_LINK_DLTS.RM_UM_UOM_CODE_PLANT  = RM_UOM_MASTER . RM_UM_UOM_CODE  (+)";


                dtSTLinkDetails = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtSTLinkDetails;
        }
        public DataTable FetchProductTypeDetails ( string RM_RMM_RM_CODE )
        {
            DataTable dtProductTypeDetails = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " ";
                SQL = SQL +"   SELECT RM_RAWMATERIAL_CONS_ACC_DTL.GL_PRODUCT_TYPE_CODE,GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_DESC,  ";
                SQL = SQL + "         RM_RAWMATERIAL_CONS_ACC_DTL.RM_RMM_CONS_ACC_CODE ,GL_COA_MASTER.GL_COAM_ACCOUNT_NAME , ";
                SQL = SQL + "         RM_RAWMATERIAL_CONS_ACC_DTL.RM_RMM_CONS_ACC_CODE_ONSITE,COA_MASTER.GL_COAM_ACCOUNT_NAME CONS_ACC_NAME_ONSITE, ";
                SQL = SQL + "         RM_RAWMATERIAL_CONS_ACC_DTL.PC_BUD_RESOURCE_CODE_PROJECT BUD_RESOURCE_CODE_PROJECT,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME_PROJECT, ";
                SQL = SQL + "         RM_RAWMATERIAL_CONS_ACC_DTL.PC_BUD_RESOURCE_CODE_ONSITE RESOURCE_CODE_ONSITE,RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME_ONSITE ";
                SQL = SQL + "    FROM RM_RAWMATERIAL_CONS_ACC_DTL,GL_PRODUCT_TYPE_MASTER, ";
                SQL = SQL + "         GL_COA_MASTER,GL_COA_MASTER COA_MASTER, ";
                SQL = SQL + "         PC_BUD_RESOURCE_MASTER,PC_BUD_RESOURCE_MASTER RESOURCE_MASTER ";
                SQL = SQL + "   where RM_RMM_RM_CODE='" + RM_RMM_RM_CODE + "' ";
                SQL = SQL + "     AND RM_RAWMATERIAL_CONS_ACC_DTL.GL_PRODUCT_TYPE_CODE=GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE  ";
                SQL = SQL + "     AND RM_RAWMATERIAL_CONS_ACC_DTL.RM_RMM_CONS_ACC_CODE=GL_COA_MASTER.GL_COAM_ACCOUNT_CODE  ";
                SQL = SQL + "     AND RM_RAWMATERIAL_CONS_ACC_DTL.RM_RMM_CONS_ACC_CODE_ONSITE=COA_MASTER.GL_COAM_ACCOUNT_CODE(+) ";
                SQL = SQL + "     AND RM_RAWMATERIAL_CONS_ACC_DTL.PC_BUD_RESOURCE_CODE_PROJECT=PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE ";
                SQL = SQL + "     AND RM_RAWMATERIAL_CONS_ACC_DTL.PC_BUD_RESOURCE_CODE_ONSITE=RESOURCE_MASTER.PC_BUD_RESOURCE_CODE ";
                SQL = SQL + "ORDER BY GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_SORTID ASC  ";

                dtProductTypeDetails = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dtProductTypeDetails;
        }

        public DataTable FetchRMDetails ( string sCode )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();



                SQL = " SELECT RM_RAWMATERIAL_DETAILS.RM_RMM_RM_CODE, RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE, SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + " 0  QTY_ON_HAND,AD_BRANCH_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME ,RM_RMM_SALES_GRANTED_YN GRANTED_YN  ";
                SQL = SQL + " FROM RM_RAWMATERIAL_DETAILS , SL_STATION_MASTER,AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE  RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE (+) ";
                SQL = SQL + " AND RM_RAWMATERIAL_DETAILS.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE(+) ";
                SQL = SQL + " and  RM_RMM_RM_CODE  ='" + sCode + "'";
                SQL = SQL + "   ORDER BY RM_RAWMATERIAL_DETAILS.AD_BR_CODE , RM_RAWMATERIAL_DETAILS.SALES_STN_STATION_CODE   ASC ";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);




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
            return dtReturn;

        }

        public DataTable FetchTaxApplicableRate ( string Type )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RMM_VAT_APPLICABLE_YN";
                SQL = SQL + " FROM TS_RM_PRODUCT_TYPE_MASTER ";
                SQL = SQL + " WHERE RM_RMM_PRODUCT_TYPE  ='" + Type + "'";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return dtReturn;

        }



        #endregion 

        #region "FILL Views"

        public DataSet FillCombo ( )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RAWMATERIAL_TYPE RAW_TYPE FROM RM_RAWMATERIAL_TYPE ORDER BY SLNO ASC";
                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                // GRADE
                dsData.Tables.Add(dtGrade);
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
            return dsData;
        }


        public DataSet FillCombo1 ( )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_UM_UOM_CODE CODE ,RM_UM_UOM_DESC NAME FROM RM_UOM_MASTER";
                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                // GRADE
                dsData.Tables.Add(dtGrade);
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
            return dsData;
        }

        public DataSet FillProdType ( )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RMM_PRODUCT_TYPE ,rm_rmm_product_type_name  FROM TS_RM_PRODUCT_TYPE_MASTER ORDER BY rm_rmm_product_type_slno  ASC";
                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtGrade);
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
            return dsData;
        }

        public DataTable FetchStation ( )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for f 


                //SQL = SQL + "SELECT BR.AD_BR_CODE,AD_BR_NAME, S.SALES_STN_STATION_CODE ,SALES_STN_STATION_NAME ,0   RM_RP_PRICE ";
                //SQL = SQL + "FROM AD_BRANCH_MASTER BR,SL_STATION_MASTER S ";
                //SQL = SQL + "WHERE( AD_BR_READYMIX_VISIBILE_YN='Y' ";
                //SQL = SQL +  "OR ";
                //SQL = SQL + " AD_BR_BLOCK_VISIBILE_YN='Y' ";
                //SQL = SQL + "OR ";
                //SQL = SQL + "AD_BR_PRECAST_VISIBILE_YN='Y') ";
                //SQL = SQL + "AND SALES_RAW_MATERIAL_STATION ='Y' ";
                //SQL = SQL + " ORDER BY AD_BR_CODE ASC";

                SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME ,0   RM_RP_PRICE ";
                SQL = SQL + " FROM ";
                SQL = SQL + "  SL_STATION_MASTER, SL_STATION_BRANCH_MAPP_DTLS_VW WHERE SALES_STN_DELETESTATUS = 0  ";
                SQL = SQL + "  AND SL_STATION_MASTER.SALES_STN_STATION_CODE = SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE ";
                SQL = SQL + "  and SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_RAW_MATERIAL_STATION ='Y'    ";
                SQL = SQL + "  ORDER BY  SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID    ASC  ";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return dtReturn;

        }
        public DataTable FetchType ( )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "SELECT  ";
                SQL = SQL + "GL_PRODUCT_TYPE_CODE, GL_PRODUCT_TYPE_DESC  ";
                SQL = SQL + "FROM GL_PRODUCT_TYPE_MASTER ";
                SQL = SQL + "order by GL_PRODUCT_TYPE_SORTID asc ";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

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
            return dtReturn;

        }

        public DataTable FillView ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE RMCODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION DESCRIPTION, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_TYPE RMTYPE ,";
                SQL = SQL + " RM_CATEGORY_MASTER.RM_CM_CATEGORY_DESC RMCAT, RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_DESC RMSUBCAT ,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE  , ";
                SQL = SQL + " PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME ,RM_RMM_RM_SHORT_NAME SHORT_NAME, ";
                SQL = SQL + " Oder_Dtls.RM_UM_UOM_DESC OrderUnit,Consumption.RM_UM_UOM_DESC ConsumptionUnit,RM_RMM_BBS_REF_CODE BBS_REF ";

                SQL = SQL + "FROM RM_CATEGORY_MASTER , RM_CATEGORY_SUB_MASTER , ";
                SQL = SQL + " RM_UOM_MASTER Oder_Dtls, RM_UOM_MASTER Consumption , RM_RAWMATERIAL_MASTER ,PC_BUD_RESOURCE_MASTER ";

                SQL = SQL + "WHERE  RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE = RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE (+)";
                SQL = SQL + "AND RM_RAWMATERIAL_MASTER.RM_CM_CATEGORY_CODE = RM_CATEGORY_MASTER.RM_CM_CATEGORY_CODE (+) ";
                SQL = SQL + "AND  RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = Oder_Dtls.RM_UM_UOM_CODE(+) ";
                SQL = SQL + "AND  RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE_CONS = Consumption.RM_UM_UOM_CODE(+) ";
                SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE= PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+) ";
                SQL = SQL + " ORDER BY  RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE DESC ";



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

        public DataTable FillView1 ( )
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_CM_CATEGORY_CODE CAT_COE, RM_CM_CATEGORY_DESC	  CATE_NAME   FROM RM_CATEGORY_MASTER ORDER BY RM_CM_CATEGORY_CODE  ASC";

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


        public DataTable FillView2 ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + "  RM_SCM_SUBCATEGORY_CODE CAT_CODE , RM_SCM_SUBCATEGORY_DESC  CAT_NAME, ";
                SQL = SQL + "  RM_CATEGORY_SUB_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME,RM_SCM_SUBCAT_CODE_PATTERN_YN  ";

                SQL = SQL + "FROM 	";
                SQL = SQL + "  RM_CATEGORY_SUB_MASTER,PC_BUD_RESOURCE_MASTER ";

                SQL = SQL + "WHERE 	";
                SQL = SQL + "  RM_CATEGORY_SUB_MASTER.PC_BUD_RESOURCE_CODE =PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+) ORDER BY RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE ASC ";

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


        public DataTable FillView3 ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RMM_RM_CODE ITEM_CODE , RM_RMM_RM_DESCRIPTION  ITEM_DESC FROM 	";
                SQL = SQL + "  RM_RAWMATERIAL_MASTER ORDER BY RM_RMM_RM_CODE  DESC ";

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

        public DataTable FillViewPlantMaster ( string sSelected )
        {
            DataTable dtPlant = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE CODE ,";
                SQL = SQL + "    TECH_PLANT_MASTER.TECH_PLM_PLANT_NAME  NAME,  TECH_PLANT_MASTER.SALES_STN_STATION_CODE STCODE ,";
                SQL = SQL + " SALES_STN_STATION_NAME STNAME";
                SQL = SQL + " FROM TECH_PLANT_MASTER, SL_STATION_MASTER ";
                SQL = SQL + " WHERE SL_STATION_MASTER.SALES_STN_STATION_CODE = TECH_PLANT_MASTER.SALES_STN_STATION_CODE";
                if (!string.IsNullOrEmpty(sSelected.ToString().Trim()))
                {
                    SQL = SQL + " AND TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE NOT IN('" + sSelected.Replace(",", "','") + "')";
                }

                SQL = SQL + " order by  TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE asc";


                dtPlant = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
            }
            return dtPlant;
        }


        public DataTable FillBudgetBreakDownView ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT PC_BUD_RESOURCE_CODE CODE, PC_BUD_RESOURCE_NAME NAME  FROM PC_BUD_RESOURCE_MASTER";

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


        #endregion 

        #region " DML " 

        public string InsertSQL ( RMMasterEntity MasterEntity, List<ItemMasterStationDet> EntityItemDetails, List<RMPlantLinkEntity> oRMLink, List<RMProductTypeEntity> ORmProdType, object mngrclassobj )

        {

            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                ////////     MISSITN DATA            INSERT INTO RM_RAWMATERIAL_CONS_ACC_DTL(
                ////////         RM_RMM_RM_CODE, GL_PRODUCT_TYPE_CODE, RM_RMM_CONS_ACC_CODE)
                //////// select RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE,GL_PRODUCT_TYPE_CODE,RM_RMM_CONS_ACC_CODE from GL_PRODUCT_TYPE_MASTER,RM_RAWMATERIAL_MASTER
                //////// WHERE GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE
                //////// NOT  IN(SELECT  GL_PRODUCT_TYPE_CODE   FROM RM_RAWMATERIAL_CONS_ACC_DTL)



                ////////    INSERT INTO  WS_ITEM_SUB_GROUP_CONS_ACC_DTL(
                ////////   WS_SCM_SUBCATEGORY_CODE, GL_PRODUCT_TYPE_CODE, WS_SCM_PROJECT_CONS_ACC,
                ////////   WS_SCM_PROJECT_CONS_ACC_ONSITE)
                ////////select WS_ITEM_SUB_GROUP_MASTER.WS_SCM_SUBCATEGORY_CODE,GL_PRODUCT_TYPE_CODE,WS_SCM_PROJECT_CONS_ACC,WS_SCM_PROJECT_CONS_ACC_ONSITE from
                //////// WS_ITEM_SUB_GROUP_MASTER,GL_PRODUCT_TYPE_MASTER
                //////// WHERE GL_PRODUCT_TYPE_MASTER.GL_PRODUCT_TYPE_CODE
                //////// NOT  IN(SELECT  GL_PRODUCT_TYPE_CODE   FROM WS_ITEM_SUB_GROUP_CONS_ACC_DTL)



                SQL = " INSERT INTO RM_RAWMATERIAL_MASTER ( ";
                SQL = SQL + "   RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION,  RM_RMM_RM_SHORT_NAME ,";
                SQL = SQL + "   RM_SCM_SUBCATEGORY_CODE,RM_CM_CATEGORY_CODE, RM_UM_UOM_CODE, RM_RMM_RM_TYPE, RM_RMM_RM_status, ";
                SQL = SQL + "   RM_RMM_INV_ACC_CODE, RM_RMM_COS_ACC_CODE, RM_RMM_CONS_ACC_CODE,  ";
                SQL = SQL + "   RM_RMM_INC_ACC_CODE,   RM_RMM_RM_REMARKS , AD_CM_COMPANY_CODE, RM_RMM_CREATEDBY,  ";
                SQL = SQL + "   RM_RMM_CREATEDDATE, RM_RMM_UPDATEDBY, RM_RMM_UPDATEDDATE , RM_RMM_MAT_DENSITY, RM_UM_UOM_CODE_CONS,";//to added RM_RMM_MAT_DENSITY by jiju on 14-nov-24
                SQL = SQL + "   RM_RMM_INV_TRANS_ACC_CODE,PC_BUD_RESOURCE_CODE,RM_RMM_BBS_REF_CODE,RM_UM_UOM_OTHER_ITEM_CONS,RM_RMM_OTHER_ITEM_CONFACT,   ";
                SQL = SQL + "   RM_RMM_FINISHED_GOODS_ACC_CODE, RM_RMM_VAT_APPLICABLE_YN, RM_RMM_PHYSICAL_STOCK_YN, RM_RMM_PRODUCT_TYPE,RM_RMM_OLD_REF_NO,RM_RMM_RM_DIAMETER,RM_RMM_RECEIPT_QTY_VARIENCE,RM_RMM_ALLOW_MINUS_QTY_YN ) ";

                SQL = SQL + "   VALUES ( '" + MasterEntity.RMCode + "' , ";
                SQL = SQL + "'" + MasterEntity.RMDesc + "' , '" + MasterEntity.RMShortDesc + "' , '" + MasterEntity.subCatCode + "', ";
                SQL = SQL + " '" + MasterEntity.CatCode + "', '" + MasterEntity.Unit + "', '" + MasterEntity.InvType + "','" + MasterEntity.rdStatus + "',";

                SQL = SQL + "'" + MasterEntity.InvAcCode + "','" + MasterEntity.CosAcCode + "','" + MasterEntity.InvCOCAcCode + "',";
                SQL = SQL + "'" + MasterEntity.IncomeAcCode + "','" + MasterEntity.Remarks + "','" + mngrclass.CompanyCode + "' ,'" + mngrclass.UserName + "' ,";
                SQL = SQL + "'" + DateTime.Now.ToString("dd-MMM-yyyy") + "','','','" + MasterEntity.RMDensity + "', '" + MasterEntity.UomConsumpt + "',";
                SQL = SQL + "'" + MasterEntity.InvTransAcCode + "','" + MasterEntity.BudgBreakCode + "','" + MasterEntity.BBSRefCode + "','" + MasterEntity.UomElementConsump + "','" + MasterEntity.ElementConfactor + "',";
                SQL = SQL + "'" + MasterEntity.FinishedGoodsInventoryAccCode + "', '" + MasterEntity.rdTaxApplicable + "', '" + MasterEntity.rdPhysicalStockYN + "', '" + MasterEntity.ProductType + "','" + MasterEntity.RMoldRefno + "'," + MasterEntity.Diameter + "," + MasterEntity.ReceiptQtyVarience + ",'" + MasterEntity.rdMiniusQtyYN + "' ) ";

                sSQLArray.Add(SQL);

                foreach (var Data in EntityItemDetails)
                {
                    if (Data.oStationcode != null)
                    {
                        // scode = code.Text;

                        SQL = " INSERT INTO RM_RAWMATERIAL_DETAILS ( ";
                        SQL = SQL + "   RM_RMM_RM_CODE, AD_CM_COMPANY_CODE, SALES_STN_STATION_CODE,  ";
                        SQL = SQL + "   RM_RMD_PRICE, RM_RMD_QTY_ON_HAND, RM_RMD_DELETESTATUS,AD_BR_CODE,RM_RMM_SALES_GRANTED_YN)  ";

                        SQL = SQL + " VALUES ('" + MasterEntity.RMCode + "' ,'" + mngrclass.CompanyCode + "' , '" + Data.oStationcode + "' ,";
                        SQL = SQL + " 0  , " + Data.dQty + "  ,0, '" + Data.oBranchcode + "' ,'" + Data.bGranted + "' )";


                        sSQLArray.Add(SQL);
                    }
                }
                InsertRMLinkDetails(MasterEntity.RMCode, oRMLink);

                InsertRMProductTypeDetails(MasterEntity.RMCode, ORmProdType);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMRMM", MasterEntity.RMCode, false, Environment.MachineName, "I", sSQLArray);
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

        public string UpdateSQL ( RMMasterEntity MasterEntity, List<ItemMasterStationDet> EntityItemDetails, List<RMPlantLinkEntity> oRMLink,
            List<RMProductTypeEntity> ORmProdType, object mngrclassobj )
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                sSQLArray.Clear();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " Update   RM_RAWMATERIAL_MASTER SET ";

                SQL = SQL + " RM_RMM_RM_DESCRIPTION= '" + MasterEntity.RMDesc + "' , RM_RMM_RM_SHORT_NAME = '" + MasterEntity.RMShortDesc + "', RM_RMM_RM_status='" + MasterEntity.rdStatus + "',";
                SQL = SQL + " RM_RMM_RM_TYPE='" + MasterEntity.InvType + "',";
                SQL = SQL + " RM_UM_UOM_CODE='" + MasterEntity.Unit + "',RM_UM_UOM_CODE_CONS=  '" + MasterEntity.UomConsumpt + "',";
                SQL = SQL + " RM_RMM_MAT_DENSITY='" + MasterEntity.RMDensity + "',";//to added on 14-nov-24 by jiju
                SQL = SQL + " RM_RMM_INV_ACC_CODE = '" + MasterEntity.InvAcCode + "',  RM_RMM_COS_ACC_CODE ='" + MasterEntity.CosAcCode + "', RM_RMM_CONS_ACC_CODE='" + MasterEntity.InvCOCAcCode + "',  ";
                SQL = SQL + " RM_RMM_INC_ACC_CODE ='" + MasterEntity.IncomeAcCode + "',";

                SQL = SQL + " RM_RMM_RM_REMARKS='" + MasterEntity.Remarks + "',";
                SQL = SQL + " RM_RMM_VAT_APPLICABLE_YN ='" + MasterEntity.rdTaxApplicable + "',";
                SQL = SQL + " RM_RMM_PHYSICAL_STOCK_YN ='" + MasterEntity.rdPhysicalStockYN + "',";
                SQL = SQL + " RM_RMM_PRODUCT_TYPE ='" + MasterEntity.ProductType + "',";
                SQL = SQL + " RM_RMM_UPDATEDBY= 'ADMIN'  ,RM_RMM_UPDATEDDATE= '" + DateTime.Now.ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + " RM_RMM_INV_TRANS_ACC_CODE=  '" + MasterEntity.InvTransAcCode + "',RM_RMM_FINISHED_GOODS_ACC_CODE='" + MasterEntity.FinishedGoodsInventoryAccCode + "' ,";
                SQL = SQL + " PC_BUD_RESOURCE_CODE ='" + MasterEntity.BudgBreakCode + "', RM_RMM_OLD_REF_NO='" + MasterEntity.RMoldRefno + "', RM_RMM_RM_DIAMETER=" + MasterEntity.Diameter + ",RM_RMM_BBS_REF_CODE='" + MasterEntity.BBSRefCode + "',RM_UM_UOM_OTHER_ITEM_CONS='" + MasterEntity.UomElementConsump + "',RM_RMM_OTHER_ITEM_CONFACT='" + MasterEntity.ElementConfactor + "', ";
                SQL = SQL + " RM_RMM_RECEIPT_QTY_VARIENCE ='" + MasterEntity.ReceiptQtyVarience + "', ";
                SQL = SQL + " RM_RMM_ALLOW_MINUS_QTY_YN ='" + MasterEntity.rdMiniusQtyYN + "'";

                SQL = SQL + " WHERE  RM_RMM_RM_CODE ='" + MasterEntity.RMCode + "'";

                sSQLArray.Add(SQL);


                foreach (var Data in EntityItemDetails)
                {
                    if (Data.oStationcode != null)
                    {
                        // scode = code.Text;

                        SQL = " UPDATE  RM_RAWMATERIAL_DETAILS SET ";
                        SQL = SQL + " RM_RMM_SALES_GRANTED_YN ='" + Data.bGranted + "'";
                        SQL = SQL + "  WHERE AD_BR_CODE ='" + Data.oBranchcode + "' AND    RM_RMM_RM_CODE = '" + MasterEntity.RMCode + "'";
                        SQL = SQL + "  AND    SALES_STN_STATION_CODE = '" + Data.oStationcode + "'";


                        sSQLArray.Add(SQL);
                    }
                }


                InsertRMLinkDetails(MasterEntity.RMCode, oRMLink);
                InsertRMProductTypeDetails(MasterEntity.RMCode, ORmProdType);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMRMM", MasterEntity.RMCode, false, Environment.MachineName, "U", sSQLArray);
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

        public string DeleteSQL ( string sCode, object mngrclassobj )
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();
                sRetun = DeleteCheck(sCode);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                SQL = " Delete from    RM_RAWMATERIAL_MASTER    ";
                SQL = SQL + " WHERE    RM_RMM_RM_CODE  ='" + sCode + "'";

                sSQLArray.Add(SQL);

                SQL = " Delete from  RM_RAWMATERIAL_DETAILS     ";
                SQL = SQL + " WHERE RM_RMM_RM_CODE  ='" + sCode + "'";

                sSQLArray.Add(SQL);

                SQL = " Delete from  RM_RAWMATERIAL_BATCH_LINK_DLTS     ";
                SQL = SQL + " WHERE RM_RMM_RM_CODE  ='" + sCode + "'";

                sSQLArray.Add(SQL);


                SQL = " Delete from  RM_RAWMATERIAL_CONS_ACC_DTL     ";
                SQL = SQL + " WHERE RM_RMM_RM_CODE  ='" + sCode + "'";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMRMM", sCode, false, Environment.MachineName, "D", sSQLArray);
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

        private string DeleteCheck ( string Code )
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 


                SQL = " SELECT    count(*) CNT   FROM RM_PR_DETAILS   where RM_PR_DETAILS.RM_RMM_RM_CODE='" + Code + "'";


                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "cannot delete, reference already exists in Purchase Request";
                }


                SQL = " SELECT    count(*) CNT   FROM RM_PO_DETAILS   where RM_PO_DETAILS.RM_RMM_RM_CODE='" + Code + "'";


                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "cannot delete, reference already exists in Purchase Order";
                }


                SQL = " SELECT    count(*) CNT   FROM   RM_STOCK_LEDGER where  RM_STOCK_LEDGER.RM_IM_ITEM_CODE='" + Code + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "cannot delete, reference already exists in Stock Ledger";
                }


                SQL = " SELECT    count(*) CNT   FROM TECH_PRODUCT_COMP_DETAILS  where  RM_RMM_RM_CODE ='" + Code + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "cannot delete, reference already exists in Product Master";
                }



            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

        private void InsertRMLinkDetails ( string RM_RMM_RM_CODE, List<RMPlantLinkEntity> oRMLink )
        {
            DataSet dsInsert = new DataSet();

            try
            {
                SqlHelper clSQLHelper = new SqlHelper();
                SQL = "DELETE FROM RM_RAWMATERIAL_BATCH_LINK_DLTS WHERE RM_RMM_RM_CODE='" + RM_RMM_RM_CODE + "'";
                sSQLArray.Add(SQL);
                foreach (var Data in oRMLink)
                {
                    if (Data.PLCode != null)
                    {
                        // scode = code.Text;

                        SQL = " insert into RM_RAWMATERIAL_BATCH_LINK_DLTS (";
                        SQL = SQL + "  RM_RMM_RM_CODE,SALES_STN_STATION_CODE,TECH_PLM_PLANT_CODE,  ";
                        SQL = SQL + "   RM_CODE_BATCHING_PLANT , RM_UM_UOM_CODE_PLANT )  ";

                        SQL = SQL + " VALUES ('" + RM_RMM_RM_CODE + "' ,'" + Data.STCode + "' , '" + Data.PLCode + "' ,'" + Data.RMPLCode + "','" + Data.RMPLUOMCode + "')";


                        sSQLArray.Add(SQL);
                    }
                }
            }
            catch (Exception ex)
            {

            }


        }
        private void InsertRMProductTypeDetails ( string RM_RMM_RM_CODE, List<RMProductTypeEntity> oRMProductType )
        {
            DataSet dsInsert = new DataSet();

            try
            {
                SqlHelper clSQLHelper = new SqlHelper();
                SQL = "DELETE FROM RM_RAWMATERIAL_CONS_ACC_DTL WHERE RM_RMM_RM_CODE='" + RM_RMM_RM_CODE + "'";
                sSQLArray.Add(SQL);
                foreach (var Data in oRMProductType)
                {
                    if (Data.ProdTypeCode != null)
                    {
                        SQL = " insert into RM_RAWMATERIAL_CONS_ACC_DTL (";
                        SQL = SQL + "  RM_RMM_RM_CODE,GL_PRODUCT_TYPE_CODE,RM_RMM_CONS_ACC_CODE,RM_RMM_CONS_ACC_CODE_ONSITE,PC_BUD_RESOURCE_CODE_PROJECT,PC_BUD_RESOURCE_CODE_ONSITE)  ";

                        SQL = SQL + " VALUES ('" + RM_RMM_RM_CODE + "','" + Data.ProdTypeCode + "','" + Data.AccountCode + "','" + Data.AccountCodePrjOnsite + "','" + Data.ResourcePrj + "','" + Data.ResourceOnsite + "' )";

                        sSQLArray.Add(SQL);
                    }
                }
            }
            catch (Exception ex)
            {

            }


        }


        #endregion


        #region "Get Item Code "
        public string GetItemCodeold ( string SubCategorycode, string CateCode )
        {
            DataTable dtproduct = new DataTable();
            SqlHelper clsSQLHelper = new SqlHelper();
            string lsCode = null;
            int liCount = 0;
            try
            {
                string CodePrefix = CateCode.ToString();
                string subgroupItemGrouping;
                //SQL = "  SELECT";
                //SQL = SQL + "   Nvl(MAX(RM_RMM_RM_CODE),0)  cnt ";
                //SQL = SQL + "   FROM RM_RAWMATERIAL_MASTER";
                //SQL = SQL + "   WHERE RM_SCM_SUBCATEGORY_CODE  = '" + SubCategory + "'";


                SQL = "  SELECT";
                SQL = SQL + "   RM_SCM_ITEM_CODE_GROUP  from RM_CATEGORY_SUB_MASTER ";
                SQL = SQL + "    where   RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode + "'";
                dtproduct = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtproduct.Rows.Count > 0)
                {
                    subgroupItemGrouping = dtproduct.Rows[0]["RM_SCM_ITEM_CODE_GROUP"].ToString();
                }
                else
                {
                    return "eeror Not defined item grouping ";
                }

                ////SQL = "     SELECT   " ;
                ////SQL = SQL + "         RM_SCM_ITEM_CODE_GROUP  ,  Nvl(MAX(RM_RMM_RM_CODE),0)  cnt  ";
                ////SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER ,RM_CATEGORY_SUB_MASTER  ";
                ////SQL = SQL + "   WHERE  ";
                ////SQL = SQL + "  RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE = RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE  ";
                ////SQL = SQL + "    and RM_RAWMATERIAL_MASTER. RM_CM_CATEGORY_CODE = '" + CateCode   + "'";
                ////SQL = SQL + "   and  RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode  + "'";
                ////SQL = SQL + "   group by   RM_SCM_ITEM_CODE_GROUP   "; 

                //  lsCode = Convert.ToString(SubCategory);

                if (subgroupItemGrouping == "5")
                {  // hard code for mace 

                    SQL = "     SELECT        ";
                    SQL = SQL + "       NVL(MAX(RM_RMM_RM_CODE),0)  CNT     ";
                    SQL = SQL + "       FROM RM_RAWMATERIAL_MASTER   ";
                    SQL = SQL + "      WHERE    RM_RMM_RM_CODE  like  '5%'  ";
                    // error will come once reach 600 infomred abin as well jomy 
                }
                else
                {
                    SQL = "     SELECT        ";
                    SQL = SQL + "       NVL(MAX(RM_RMM_RM_CODE),0)  CNT     ";
                    SQL = SQL + "       FROM RM_RAWMATERIAL_MASTER   ";
                    SQL = SQL + "      WHERE  RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode + "' and   ";
                    SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE IN ";
                    SQL = SQL + "       (  ";
                    SQL = SQL + "      SELECT   RM_SCM_SUBCATEGORY_CODE   FROM RM_CATEGORY_SUB_MASTER ";
                    SQL = SQL + "      WHERE  RM_SCM_ITEM_CODE_GROUP =   '" + subgroupItemGrouping + "' ";
                    SQL = SQL + "     )  ";
                }


                dtproduct = clsSQLHelper.GetDataTableByCommand(SQL);



                liCount = Convert.ToInt32(dtproduct.Rows[0]["cnt"].ToString());


                if (liCount.ToString() == "0")
                {
                    lsCode = CodePrefix + subgroupItemGrouping + "0" + Convert.ToString(liCount + 1);

                }
                else
                {
                    //automaticlly will take next 1 
                    lsCode = Convert.ToString(liCount + 1);
                }




            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }

            return lsCode;
        }


        public string GetItemCode ( string SubCategorycode, string CateCode )
        {
            DataTable dtproduct = new DataTable();
            SqlHelper clsSQLHelper = new SqlHelper();
            string lsCode = null;
            int liCount = 0;
            try
            {
                string CodePrefix = CateCode.ToString();
                string subgroupItemGrouping;
                //SQL = "  SELECT";
                //SQL = SQL + "   Nvl(MAX(RM_RMM_RM_CODE),0)  cnt ";
                //SQL = SQL + "   FROM RM_RAWMATERIAL_MASTER";
                //SQL = SQL + "   WHERE RM_SCM_SUBCATEGORY_CODE  = '" + SubCategory + "'";


                SQL = "  SELECT";
                SQL = SQL + "   RM_SCM_ITEM_CODE_GROUP  from RM_CATEGORY_SUB_MASTER ";
                SQL = SQL + "    where   RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode + "'";
                dtproduct = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtproduct.Rows.Count > 0)
                {
                    subgroupItemGrouping = dtproduct.Rows[0]["RM_SCM_ITEM_CODE_GROUP"].ToString();
                }
                else
                {
                    return "eeror Not defined item grouping ";
                }



                SQL = "     SELECT        ";
                SQL = SQL + "       NVL(MAX(RM_RMM_RM_CODE),0)  CNT     ";
                SQL = SQL + "       FROM RM_RAWMATERIAL_MASTER   ";
                SQL = SQL + "      WHERE  RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode + "' and   ";
                SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE IN ";
                SQL = SQL + "       (  ";
                SQL = SQL + "      SELECT   RM_SCM_SUBCATEGORY_CODE   FROM RM_CATEGORY_SUB_MASTER ";
                SQL = SQL + "      WHERE  RM_SCM_ITEM_CODE_GROUP =   '" + subgroupItemGrouping + "' ";
                SQL = SQL + "     )  ";



                dtproduct = clsSQLHelper.GetDataTableByCommand(SQL);



                liCount = Convert.ToInt32(dtproduct.Rows[0]["cnt"].ToString());


                if (liCount.ToString() == "0")
                {
                    lsCode = CodePrefix + subgroupItemGrouping + "0" + Convert.ToString(liCount + 1);

                }
                else
                {
                    //automaticlly will take next 1 
                    lsCode = Convert.ToString(liCount + 1);
                }




            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }

            return lsCode;
        }


        public string GetItemCodeHPBS ( string CatCode, string subCatCode )
        {
            DataSet dsData = new DataSet();
            DataTable dsItemCode = new DataTable();
            DataTable dtcatPrefix = new DataTable();
            DataTable dtSubcatPrefix = new DataTable();
            string lsCode = null;

            int liCount = 0;
            string Main = null;
            string Cat = null;
            string SubCat = null;


            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "select RM_CM_CATEGORY_PREFIX from RM_CATEGORY_MASTER where RM_CM_CATEGORY_CODE='" + CatCode + "'";

                dtcatPrefix = clsSQLHelper.GetDataTableByCommand(SQL);


                SQL = "select RM_SCM_ITEM_CODE_GROUP from RM_CATEGORY_SUB_MASTER where RM_SCM_SUBCATEGORY_CODE='" + subCatCode + "'";

                dtSubcatPrefix = clsSQLHelper.GetDataTableByCommand(SQL);


                //if(scompanyCode == "MACEQ")
                //{
                // AB-AB-ABC-0000  format
                //MainPrefix-CategoryPrefix-SubCategoryPrefix-Max+1


                // GIBIN MADE WROING STATEMENT AND CORRETED BY JOMY 07 DEC 2018 // NVL(MAX (SUBSTR (WS_IM_ITEM_CODE, 8, 4)), 0)

                //SQL = "     SELECT NVL(MAX (SUBSTR (WS_IM_ITEM_CODE, 11, 10)), 0) cnt";
                //SQL = SQL + "   FROM WS_ITEM_MASTER";
                //SQL = SQL + "    WHERE WS_CM_CATEGORY_CODE = '" + CatCode + "'";
                //SQL = SQL + "    AND WS_SCM_SUBCATEGORY_CODE = '" + subCatCode + "'";

                SQL = "     SELECT  NVL(MAX(SUBSTR(RM_RMM_RM_CODE, 13, 4)), 0)  cnt";
                SQL = SQL + "   FROM RM_RAWMATERIAL_MASTER ";
                SQL = SQL + "    WHERE RM_CM_CATEGORY_CODE = '" + CatCode + "'";
                SQL = SQL + "    AND RM_SCM_SUBCATEGORY_CODE = '" + subCatCode + "'";


                dsItemCode = clsSQLHelper.GetDataTableByCommand(SQL);





                //if (dsItemCode.Rows.Count > 0)
                //{
                liCount = Convert.ToInt16(dsItemCode.Rows[0]["cnt"].ToString());
                if (liCount < 9)
                {
                    lsCode = lsCode + "000" + Convert.ToString(liCount + 1);
                }
                else if (liCount < 99)
                {
                    lsCode = lsCode + "00" + Convert.ToString(liCount + 1);
                }
                else if (liCount < 999)
                {
                    lsCode = lsCode + "0" + Convert.ToString(liCount + 1);
                }
                else if (liCount >= 999)
                {
                    lsCode = lsCode + Convert.ToString(liCount + 1);
                }
                // }

                //if (dtPrefix.Rows.Count > 0)
                //{
                //  Main = dtPrefix.Rows[0]["MAIN"].ToString();
                Cat = dtcatPrefix.Rows[0]["RM_CM_CATEGORY_PREFIX"].ToString();
                SubCat = dtSubcatPrefix.Rows[0]["RM_SCM_ITEM_CODE_GROUP"].ToString();

                lsCode = Cat + "-" + SubCat + "-" + subCatCode + lsCode;


                //}
                //else
                //{
                //    lsCode = ""; // error should not allow to save // jomy 
                //}



                // }

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
            return lsCode;
        }

        public string GetItemCodeADPC ( string CatCode, string subCatCode )
        {
            DataSet dsData = new DataSet();
            DataTable dsItemCode = new DataTable();
            DataTable dtcatPrefix = new DataTable();
            DataTable dtSubcatPrefix = new DataTable();
            string lsCode = null;

            int liCount = 0;
            string Main = null;
            string Cat = null;
            string SubCat = null;


            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "select RM_CM_CATEGORY_PREFIX from RM_CATEGORY_MASTER where RM_CM_CATEGORY_CODE='" + CatCode + "'";

                dtcatPrefix = clsSQLHelper.GetDataTableByCommand(SQL);


                SQL = "select RM_SCM_ITEM_CODE_GROUP from RM_CATEGORY_SUB_MASTER where RM_SCM_SUBCATEGORY_CODE='" + subCatCode + "'";

                dtSubcatPrefix = clsSQLHelper.GetDataTableByCommand(SQL);


                //if(scompanyCode == "MACEQ")
                //{
                // AB-AB-ABC-0000  format
                //MainPrefix-CategoryPrefix-SubCategoryPrefix-Max+1


                // GIBIN MADE WROING STATEMENT AND CORRETED BY JOMY 07 DEC 2018 // NVL(MAX (SUBSTR (WS_IM_ITEM_CODE, 8, 4)), 0)

                //SQL = "     SELECT NVL(MAX (SUBSTR (WS_IM_ITEM_CODE, 11, 10)), 0) cnt";
                //SQL = SQL + "   FROM WS_ITEM_MASTER";
                //SQL = SQL + "    WHERE WS_CM_CATEGORY_CODE = '" + CatCode + "'";
                //SQL = SQL + "    AND WS_SCM_SUBCATEGORY_CODE = '" + subCatCode + "'";

                SQL = "     SELECT  NVL(MAX(SUBSTR(RM_RMM_RM_CODE,  5, 4)), 0)  cnt";
                SQL = SQL + "   FROM RM_RAWMATERIAL_MASTER ";
                SQL = SQL + "    WHERE RM_CM_CATEGORY_CODE = '" + CatCode + "'";
                SQL = SQL + "    AND RM_SCM_SUBCATEGORY_CODE = '" + subCatCode + "'";


                dsItemCode = clsSQLHelper.GetDataTableByCommand(SQL);





                //if (dsItemCode.Rows.Count > 0)
                //{
                liCount = Convert.ToInt16(dsItemCode.Rows[0]["cnt"].ToString());
                if (liCount < 9)
                {
                    lsCode = lsCode + "000" + Convert.ToString(liCount + 1);
                }
                else if (liCount < 99)
                {
                    lsCode = lsCode + "00" + Convert.ToString(liCount + 1);
                }
                else if (liCount < 999)
                {
                    lsCode = lsCode + "0" + Convert.ToString(liCount + 1);
                }
                else if (liCount >= 999)
                {
                    lsCode = lsCode + Convert.ToString(liCount + 1);
                }
                // }

                //if (dtPrefix.Rows.Count > 0)
                //{
                //  Main = dtPrefix.Rows[0]["MAIN"].ToString();
                Cat = dtcatPrefix.Rows[0]["RM_CM_CATEGORY_PREFIX"].ToString();
                SubCat = dtSubcatPrefix.Rows[0]["RM_SCM_ITEM_CODE_GROUP"].ToString();

                lsCode = SubCat + "-" + lsCode;


                //}
                //else
                //{
                //    lsCode = ""; // error should not allow to save // jomy 
                //}



                // }

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
            return lsCode;
        }




        public string GetItemCodeEB ( string SubCategorycode, string CateCode )
        {
            DataTable dtproduct = new DataTable();
            SqlHelper clsSQLHelper = new SqlHelper();
            string lsCode = null;
            int liCount = 0;
            try
            {
                string CodePrefix = CateCode.ToString();
                string subgroupItemGrouping;
                //SQL = "  SELECT";
                //SQL = SQL + "   Nvl(MAX(RM_RMM_RM_CODE),0)  cnt ";
                //SQL = SQL + "   FROM RM_RAWMATERIAL_MASTER";
                //SQL = SQL + "   WHERE RM_SCM_SUBCATEGORY_CODE  = '" + SubCategory + "'";


                SQL = "  SELECT";
                SQL = SQL + "   RM_SCM_ITEM_CODE_GROUP  from RM_CATEGORY_SUB_MASTER ";
                SQL = SQL + "    where   RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode + "'";
                dtproduct = clsSQLHelper.GetDataTableByCommand(SQL);

                if (dtproduct.Rows.Count > 0)
                {
                    subgroupItemGrouping = dtproduct.Rows[0]["RM_SCM_ITEM_CODE_GROUP"].ToString();
                }
                else
                {
                    return "eeror Not defined item grouping ";
                }


                // FOR EB  // now its mixed up there for we could not contrll only ased on the sub grop prix jomyu once error is gettin need to add anotehr condition if completed  the slot // jomy 12 aug 
                // if (  subgroupItemGrouping == "5"  ||  subgroupItemGrouping == "1" 
                //   ) 
                {  // hard code for mace 

                    SQL = "     SELECT        ";
                    SQL = SQL + "       NVL(MAX(RM_RMM_RM_CODE),0)  CNT     ";
                    SQL = SQL + "       FROM RM_RAWMATERIAL_MASTER   ";
                    SQL = SQL + "      WHERE    RM_RMM_RM_CODE  like  '" + subgroupItemGrouping + "%'";
                    //  SQL = SQL + "      WHERE    RM_RMM_RM_CODE  like  '5%'  ";
                    // error will come once reach 600 infomred abin as well jomy 
                }

                ////else
                ////{
                ////    SQL = "     SELECT        ";
                ////    SQL = SQL + "       NVL(MAX(RM_RMM_RM_CODE),0)  CNT     ";
                ////    SQL = SQL + "       FROM RM_RAWMATERIAL_MASTER   ";
                ////    SQL = SQL + "      WHERE  RM_SCM_SUBCATEGORY_CODE  =  '" + SubCategorycode + "' and   ";
                ////    SQL = SQL + "       RM_RAWMATERIAL_MASTER.RM_SCM_SUBCATEGORY_CODE IN ";
                ////    SQL = SQL + "       (  ";
                ////    SQL = SQL + "      SELECT   RM_SCM_SUBCATEGORY_CODE   FROM RM_CATEGORY_SUB_MASTER ";
                ////    SQL = SQL + "      WHERE  RM_SCM_ITEM_CODE_GROUP =   '" + subgroupItemGrouping + "' ";
                ////    SQL = SQL + "     )  ";
                ////}


                dtproduct = clsSQLHelper.GetDataTableByCommand(SQL);



                liCount = Convert.ToInt32(dtproduct.Rows[0]["cnt"].ToString());


                if (liCount.ToString() == "0")
                {
                    lsCode = CodePrefix + subgroupItemGrouping + "0" + Convert.ToString(liCount + 1);

                }
                else
                {
                    //automaticlly will take next 1 
                    lsCode = Convert.ToString(liCount + 1);
                }




            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }

            return lsCode;
        }

        #endregion
    }

    public class ItemMasterStationDet
    {
        public string oStationcode { get; set; }
        public string oBranchcode { get; set; }
        public string bGranted { get; set; }

        public double dQty { get; set; }
    }

    public class RMMasterEntity
    {
        public string RMCode { get; set; }
        public string RMDesc { get; set; }
        public string RMShortDesc { get; set; }
        public string CatCode { get; set; }
        public string subCatCode { get; set; }
        public string BudgBreakCode { get; set; }
        public string Unit { get; set; }
        public string InvType { get; set; }
        public string rdStatus { get; set; }
        public string InvAcCode { get; set; }
        public string CosAcCode { get; set; }
        public string InvTransAcCode { get; set; }
        public string InvCOCAcCode { get; set; }
        public string IncomeAcCode { get; set; }

        public string rdTaxApplicable { get; set; }
        public string rdPhysicalStockYN { get; set; }
        public string ProductType { get; set; }
        public string rdMiniusQtyYN { get; set; }

        public string FinishedGoodsInventoryAccCode { get; set; }
        public string Remarks { get; set; }
        public string UomConsumpt { get; set; }
        public string BBSRefCode { get; set; }
        public string UomElementConsump { get; set; }
        public string RMoldRefno { get; set; }
        public double Diameter { get; set; }
        public int ElementConfactor { get; set; }
        public double RMDensity { get; set; }
        public double ReceiptQtyVarience { get; set; }

    }

    public class RMPlantLinkEntity
    {
        public string RMCode { get; set; }
        public string PLCode { get; set; }
        public string STCode { get; set; }
        public string RMPLCode { get; set; }
        public string RMPLUOMCode { get; set; }
    }
    public class RMProductTypeEntity
    {
        public string ProdTypeCode { get; set; }
        public string AccountCode { get; set; }
        public string AccountCodePrjOnsite { get; set; }
        public string ResourcePrj { get; set; }
        public string ResourceOnsite { get; set; }
    }

}