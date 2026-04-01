/*
==================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
==================================================================================
*/


drop procedure bronze.load_bronze
CREATE PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @END_time DATETIME , @start_batch_time DATETIME,  @end_batch_time DATETIME
	BEGIN
	TRY
		/*	BULK INSERT */
		SET @start_batch_time = GETDATE();


		SET  @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM	'C:\cust_info.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
		);
		SET @END_time =  GETDATE(); 
		PRINT '++ Load duration: ' + cast(datediff(second,@start_time,@END_time) as nvarchar) + 'seconds';
		print '-----------------------------------'


		SET  @start_time = GETDATE();
		TRUNCATE TABLE bronze.prd_info
		BULK INSERT bronze.prd_info
		FROM	'C:\prd_info.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
		);
		SET @END_time =  GETDATE();
		PRINT '++ Load duration: ' + cast(datediff(second,@start_time,@END_time) as nvarchar) + 'seconds';
		print '-----------------------------------'


			SET  @start_time = GETDATE();
		TRUNCATE TABLE bronze.sales_details
		BULK INSERT bronze.sales_details
		FROM	'C:\sales_details.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
		);
		SET @END_time =  GETDATE();
		PRINT '++ Load duration: ' + cast(datediff(second,@start_time,@END_time) as nvarchar) + 'seconds';
		print '-----------------------------------'



		SET  @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM	'C:\CUST_AZ12.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
		);
		SET @END_time =  GETDATE(); 
		PRINT '++ Load duration: ' + cast(datediff(second,@start_time,@END_time) as nvarchar) + 'seconds';
		print '-----------------------------------'



		SET  @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101
		BULK INSERT bronze.erp_LOC_A101
		FROM	'C:\LOC_A101.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
		);
		SET @END_time =  GETDATE();
		PRINT '++ Load duration: ' + cast(datediff(second,@start_time,@END_time) as nvarchar) + 'seconds';
		print '-----------------------------------'




		SET  @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM	'C:\PX_CAT_G1V2.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
		);
		SET @END_time =  GETDATE();
		PRINT '++ Load duration: ' + cast(datediff(second,@start_time,@END_time) as nvarchar) + 'seconds';
		print '-----------------------------------'

		SET  @end_batch_time = GETDATE();


		
		PRINT '======================================'
		PRINT '++ Load duration: ' + cast(datediff(second,@start_batch_time,@end_batch_time) as nvarchar) + 'seconds';
		PRINT '======================================'


	END TRY
BEGIN CATCH

PRINT '======================================'
PRINT 'ERROR OCCURED DURING LOADING UP BRONZE LAYER'
PRINT 'ERROR MESSEAGE' + Error_message();
PRINT 'ERROR MESSEAGE' + cast (Error_number() as nvarchar(50));
PRINT 'ERROR MESSEAGE' + cast (Error_state() as nvarchar(50));
PRINT '======================================'
END CATCH
END;
