using System;
using System.Data; 
using AccosoftUtilities;  
using AccosoftLogWriter; 
using AccosoftNineteenCDL;   
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class TrailerDriverAllocation
    {

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;
               

        public DataTable FillView(string col,string selectedItemes )// Fill LookUps
        {

            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                              
                if (col == "0")
                {
                    SQL = " SELECT   fa_fam_asset_code Code, fa_fam_asset_description ||'-Plate No'|| FA_FAM_PLATENO Name,";
                  SQL = SQL + " fa_fam_ref_code assetref, FA_FAM_ASSET_ID  assetid";
                  SQL = SQL + "     FROM fa_fixed_asset_master";
                  SQL = SQL + " where  FA_FAM_APPROVED='Y' and  FA_FAM_ASSET_STATUS ='A' AND  FA_FAM_VEHICLE_TYPE";
                  SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                  SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' ) ";
                  if (!string.IsNullOrEmpty(selectedItemes.Trim()))
                  {
                      SQL = SQL + "    and    fa_fam_asset_code not in ( '" + selectedItemes.Replace(",", "','") + "') ";


                  } 
                    SQL = SQL + " ORDER BY fa_fam_asset_code";
                }
                else if  (col == "2")  
                {
                     SQL = " SELECT   hr_emp_employee_code Code, hr_emp_employee_name Name,";
                     SQL = SQL + "          ad_cm_company_code , HR_EMP_EMPLOYEE_REF_CODE assetref , HR_EMP_MOBILENO assetid";
                     SQL = SQL +"     FROM hr_employee_master";
                     SQL = SQL + " WHERE  HR_EMP_STATUS ='A'";
                     SQL = SQL + " AND   HR_DM_DESIGNATION_CODE ";
                     SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                     SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_DESIG_CODE' ) ";
                     if (!string.IsNullOrEmpty(selectedItemes.Trim()))
                     {
                    SQL = SQL + "    and    hr_emp_employee_code not in ( '"  + selectedItemes.Replace(",", "','")  + "') " ; 

                     } 
                     SQL = SQL + " ORDER BY hr_emp_employee_code ASC";
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

        public DataSet FetchData(string DN,  string sALL )
        {
            DataSet dtReturn = new DataSet();
            //DataTable dtReturn = new DataTable();
            //DataTable dtRet = new DataTable();
            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                if (DN == "DN")
                {
                    if (sALL == "NO")
                    {

                        SQL = "SELECT RM_DR_SLNO,RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE,RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE , ";
                        SQL = SQL + "FA_FAM_ASSET_DESCRIPTION,HR_EMP_EMPLOYEE_NAME";
                        SQL = SQL + " FROM RM_DRIVER_MASTER_DETS,FA_FIXED_ASSET_MASTER,HR_EMPLOYEE_MASTER WHERE ";
                        SQL = SQL + " RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE ";
                        SQL = SQL + " AND RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE ";
                        SQL = SQL + " ORDER BY RM_DR_SLNO";
                    }
                    else
                    {
                        SQL = " SELECT  ";
                        SQL = SQL + "    ASM. FA_FAM_ASSET_CODE, ASM.FA_FAM_ASSET_DESCRIPTION , ";
                        SQL = SQL + "    TM.HR_EMP_EMPLOYEE_CODE, TM.HR_EMP_EMPLOYEE_NAME  ";
                        SQL = SQL + "    FROM  ";
                        SQL = SQL + "    ( ";
                        SQL = SQL + "    SELECT RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE,HR_EMP_EMPLOYEE_NAME,FA_FAM_ASSET_CODE ";
                        SQL = SQL + "    FROM  ";
                        SQL = SQL + "    RM_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER ";
                        SQL = SQL + "    WHERE RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE";                             
                        SQL = SQL + "    )  TM,";

                        SQL = SQL + "    ( ";
                        SQL = SQL + "    SELECT FA_FAM_ASSET_CODE , FA_FAM_ASSET_DESCRIPTION  ||'- Plate No '|| FA_FAM_PLATENO FA_FAM_ASSET_DESCRIPTION  ";                     
                        SQL = SQL + "    FROM FA_FIXED_ASSET_MASTER ";
                        SQL = SQL + "    WHERE  FA_FAM_APPROVED='Y' AND  FA_FAM_ASSET_STATUS ='A' AND  FA_FAM_VEHICLE_TYPE ";

                        SQL = SQL + "   IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                        SQL = SQL + "    WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' ) ";
                        SQL = SQL + "    )ASM  ";
                        SQL = SQL + "    where  asm. fa_fam_asset_code  = tm.FA_FAM_ASSET_CODE (+) ";
                        SQL = SQL + "    ORDER BY  ASM.FA_FAM_ASSET_CODE   ";
                    } 
                }
             
                dtReturn = clsSQLHelper.GetDataset(SQL);
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

        public string InsertSql( List<DriverAllocationFPSEntity> objFPSEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (objFPSEntity.Count > 0)
                {
                    // deletion before insertiong based  on station code 
                    SQL = " DELETE FROM RM_DRIVER_MASTER_DETS";                    
                    sSQLArray.Add(SQL);
                }
                int i = 0;

                foreach (var Data in objFPSEntity)
                {
                    SQL = "";

                        i++;
                        SQL = "    INSERT INTO RM_DRIVER_MASTER_DETS (";
                        SQL = SQL + "    RM_DR_SLNO, FA_FAM_ASSET_CODE, HR_EMP_EMPLOYEE_CODE";
                        SQL = SQL + "     ) ";

                        SQL = SQL + " VALUES (" + i++ + " ,'" + Data.AssetCode + "',";
                        SQL = SQL + "'" + Data.EmpCode + "'";                       
                        SQL = SQL + " )";

                        sSQLArray.Add(SQL);
                     }                   
             
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMDRIVER", "RMDRIVER", false, Environment.MachineName, "U", sSQLArray);

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
   
    
    }
    public class DriverAllocationFPSEntity
    {
        
        public object AssetCode { get; set; }

        public object EmpCode { get; set; }

        
    }
}
