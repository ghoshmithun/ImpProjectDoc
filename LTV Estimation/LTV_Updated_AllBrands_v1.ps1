$DATEINDEX = 20130101; $BRAND = "ON"; $COUNTRY="US";

$PGMMODIFIED=$("\\10.8.8.51\LV0\TANUMOY\DATASETS\MODEL REPLICATION\" + $BRAND + "_" + $DATEINDEX + "_LTV_CALIB_TRANSACTION.TXT")
 
ECHO $PGMMODIFIED

$INFILEPATH=$("\\10.8.8.51\LV0\TANUMOY\DATASETS\FROM HIVE\" + $BRAND + "_" + $DATEINDEX + "_LTV_CALIB_TRANSACTION_TXT")

ECHO $INFILEPATH

IF (TEST-PATH $PGMMODIFIED)
{
  REMOVE-ITEM $PGMMODIFIED
}
 
$FILES = GET-CHILDITEM $INFILEPATH

ECHO $FILES

FOR ($I=0; $I -LT $FILES.COUNT; $I++) 
{
 $PGM = $FILES[$I].FULLNAME 
    
 ECHO $PGM
    
 IF (TEST-PATH $PGM)
 {
     ECHO (GET-ITEM $PGM).LENGTH
     
     IF ((GET-ITEM $PGM).LENGTH -EQ 0)
     
     {
       REMOVE-ITEM $PGM
         
     }

 }
 
}





$FILES = GET-CHILDITEM $INFILEPATH

ECHO $FILES


FOR ($I=0; $I -LT $FILES.COUNT; $I++) 
{
 $PGM = $FILES[$I].FULLNAME 
    
 ECHO $PGM
    
 IF (TEST-PATH $PGM)
 {
     ECHO (GET-ITEM $PGM).LENGTH
     
     IF ((GET-ITEM $PGM).LENGTH -GT 0)
     
     {
       
    	 IF ($I -EQ 0)
    	 {
    	  (GET-CONTENT $PGM) -REPLACE "\\N", "" | SET-CONTENT $PGMMODIFIED
    	 }
    	 
    	 IF ($I -GT 0)
    	 {
    	  (GET-CONTENT $PGM) -REPLACE "\\N", "" | ADD-CONTENT $PGMMODIFIED
    	 }
    	 
    	 REMOVE-ITEM $PGM
         
     }

 }
 
}