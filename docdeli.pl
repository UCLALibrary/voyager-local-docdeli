#!/m1/shared/bin/perl
# above not needed in windows
#
#   oe.pl
#
#	OrionExpress
#
#	- read Voyager data file
#	- parse file
#	- retreive data from oracle voyager
#	- piece together emails (one per bib id) 
#	- send emails
#
#	040716
#
#	- docdeli.pl
#	- cleanup
#
#	040715
#
#	- oe5.pl
#	- added ^m and ^k removal code
#	- added report file creation code
#	- not: if the data ever contain "|", the program may break
#	- remove the ########## number ext from archived source files to reprocess
#
# 20070209 akohler: changed $sql1 query to fix error when patron has multiple WDDS categories
# 20080513 akohler: changed $cc_email to use lit-libweb
# 20080705 akohler: changed to /m1/shared/bin/perl
#
#######################################################################################################

use DBI;

# Get LD_LIBRARY_PATH from calling shell script (or existing environment?) instead of setting it here

###################### read file

### Voyager server
$rfile[0] = "/m1/voyager/ucladb/rpt/opacrequests.OE_LN.out";
$rfile[1] = "/m1/voyager/ucladb/rpt/opacrequests.OE_PH.out";
# test ===> NOTE - USE LN/PH.out EXTENSION ON TEST FILES <===

#### auto named transaction file 1 (uncomment to re-activate)
$trans = "/m1/voyager/ucladb/rpt/transactions.txt";

$t_email = "GEN_ucill\@vdxhost.com";
$cc_email = "jcl\@library.ucla.edu lit-libweb\@library.ucla.edu";
$subject="Document_Request";
$ClientLocation = 0;
$db_flag = 0;
foreach( @rfile ){
	if( -e $_ ){
		$db_flag = 1;
		}
	}

if( $db_flag == 1 ){
	# Maybe get ORACLE_SID=VGER from calling script/environment instead 
	#   of setting it here
	$dbh = DBI->connect ('DBI:Oracle:VGER', 'ucla_preaddb', 'ucla_preaddb');
	}

$rcnt = 0;

#### auto named transaction file 2 (uncomment to re-activate)
open( TRANS, ">$trans" ) || die "could not open file listing requests: $trans\n";

foreach( @rfile ){

$onelongline = '';

if( -e $_ ){

	if( /LN\.out$/ ){ $rcnt = 0; }
	elsif( /PH\.out$/ ){ $rcnt = 1; }

open( RFILE1, "$_" ) || die "could not open file listing requests: $_\n";
        @rfile1 = <RFILE1>;
        close( RFILE1 );

# for each line in rfile1
foreach( @rfile1 ){
	$onelongline = $onelongline.$_;
}

$temp_record_sep = 's67sd67sd7';
$onelongline =~ s/\x0b/ /g; # ^k - VT - 013 octal - 0b hex
$onelongline =~ s/\s\s//g; # Strip repeated spaces

# Correct input is one+ records, each with 21 fields, delimited by '|'
# 0|1|...|20\n0|1|...|20\n etc.
# Some fields other than #20 may have line breaks - incorrectly - but
# we only care about the one at the end of the final field of each record.
# Find the good line break (\n) at end of each record:
# e.g., replace \n with $temp_record_sep in |1234\n5678|

$onelongline =~ s/(\|[0-9]+)\n([0-9]+\|)/\1$temp_record_sep\2/g;
# Now strip all remaining line breaks
$onelongline =~ s/\R//g; # Strip all line breaks

@rfile1c = split($temp_record_sep, $onelongline);

foreach( @rfile1c ){

	$rfile1 = $_;

	#split line on delimiter
	@rfile1_split = split('\|', $rfile1);

	$bib_id = $rfile1_split[8];
	$patron_id = $rfile1_split[16];
	$patron_id_sql = $rfile1_split[20];

# Better logging to stdout, captured by wrapper script
print"bib_id ===> $bib_id\n";
print"patron_id ===> $patron_id\n";
print"patron_id_sql ===> $patron_id_sql\n";

#### auto named transaction file 3 (uncomment to re-activate)
print( TRANS "file type ===> $rcnt\n" );
print( TRANS "bib_id ===> $bib_id\n" );
print( TRANS "patron_id ===> $patron_id\n" );

# LN_PH data translation
	SWITCH: {
		if( $rfile1_split[18] =~ /UADD/ ){ $rfile1_split[18]="FACUL"; last SWITCH; }
		if( $rfile1_split[18] =~ /UALAD/ ){ $rfile1_split[18]="FACUL"; last SWITCH; }
		if( $rfile1_split[18] =~ /UANLDD/ ){ $rfile1_split[18]="FACUL"; last SWITCH; }
		if( $rfile1_split[18] =~ /UAPDD/ ){ $rfile1_split[18]="FACUL"; last SWITCH; }
		if( $rfile1_split[18] =~ /UGDD/ ){ $rfile1_split[18]="GRADU"; last SWITCH; }
		if( $rfile1_split[18] =~ /UGLDD/ ){ $rfile1_split[18]="GRADU"; last SWITCH; }
		if( $rfile1_split[18] =~ /UGMDD/ ){ $rfile1_split[18]="GRADU"; last SWITCH; }
		if( $rfile1_split[18] =~ /UGNDD/ ){ $rfile1_split[18]="GRADU"; last SWITCH; }
		if( $rfile1_split[18] =~ /UUDD/ ){ $rfile1_split[18]="UNDRG"; last SWITCH; }
		if( $rfile1_split[18] =~ /USDD/ ){ $rfile1_split[18]="PATRN"; last SWITCH; }
		if( $rfile1_split[18] =~ /USLDD/ ){ $rfile1_split[18]="PATRN"; last SWITCH; }
		}


# PATRON
$sql1 = "
SELECT
	P.Patron_ID,
	P.last_name,
	P.first_name,
	PA.address_type,
	PA.city AS ClientAddr4City,
	PA.state_province AS ClientAddr4Region,
	PA.zip_postal AS ClientAddr4Code,
	(select phone_number from patron_phone where address_id = PA.address_id and
       phone_type = 1 ) AS ClientAddr4Phone,
	(SELECT PSC1.patron_stat_desc FROM patron_stats PS1
       INNER JOIN patron_stat_code PSC1
       ON PS1.patron_stat_id = PSC1.patron_stat_id
       WHERE PS1.patron_id = P.patron_id
       AND  PSC1.patron_stat_desc LIKE 'YDDS%')
          AS ClientLocation,
    (SELECT PSC2.patron_stat_desc FROM patron_stats PS2
       INNER JOIN patron_stat_code PSC2
       ON PS2.patron_stat_id = PSC2.patron_stat_id
       WHERE PS2.patron_id = P.patron_id
       AND  PSC2.patron_stat_desc LIKE 'WDDS%'
       AND ROWNUM < 2)
          AS PickupLocation,
	PA.address_line1 AS ClientAddr4Street,
	(select address_line1 FROM Patron_Address where patron_id = P.patron_id and
        address_type = 3 ) AS ClientEmailAddress
FROM
    Patron P
INNER JOIN Patron_Address PA
	  ON P.Patron_ID = PA.Patron_ID
WHERE PA.Address_Type = 1
AND P.Patron_ID=$patron_id_sql
";


#BIB
$sql2 = "
SELECT  BT.bib_id,
	BT.author,
	BT.title,
	BT.edition,
	BT.publisher AS ReqPublisher,
	BT.publisher_date AS ReqPubDate,
	BT.isbn AS ReqISBN,
	BT.issn AS ReqISSN,
	BT.pub_place AS ReqPubPlace,
	MM.location_id,
	MM.display_call_no,
	L.location_code
FROM  Bib_Text BT
	INNER JOIN Bib_MFHD BM ON (BT.Bib_ID = BM.Bib_ID)
	INNER JOIN MFHD_Master MM ON (BM.MFHD_ID = MM.MFHD_ID)
	INNER JOIN Location L ON (MM.Location_ID = L.Location_ID)
WHERE BT.bib_id=( $bib_id )
";

# PATRON Query
# PATRON Query
# PATRON Query
$dd = $dbh->prepare( $sql1 );
$dd->execute();
($PATRON_ID, $LAST_NAME, $FIRST_NAME, $ADRESS_TYPE, $ClientAddr4City, $ClientAddr4Region, $ClientAddr4Code, $ClientAddr4Phone, $ClientLocation, $PickupLocation, $ClientAddr4Street, $ClientEmailAddress) = $dd->fetchrow;

$PickupLocation2 = $PickupLocation;
if( $PickupLocation2 =~ /^Web/i ){
	$PickupLocation2 = 6;
	}
else{
	$PickupLocation2 = "";
	}

# test
#print"PATRON_ID===>$PATRON_ID, LAST_NAME===>$LAST_NAME, FIRST_NAME===>$FIRST_NAME, ADRESS_TYPE===>$ADRESS_TYPE, ClientAddr4City===>$ClientAddr4City, ClientAddr4Region===>$ClientAddr4Region, ClientAddr4Code===>$ClientAddr4Code, ClientAddr4Phone===>$ClientAddr4Phone, ClientLocation===>$ClientLocation, PickupLocation===>$PickupLocation, ClientAddr4Street===>$ClientAddr4Street, ClientEmailAddress===>$ClientEmailAddress\n";


# BIB Query
# BIB Query
# BIB Query
$dd = $dbh->prepare( $sql2 );
$dd->execute();
$i = 1; # start at one to match Rota convention (first instance is set to 1)
$ula7_flag = 0;
$ula8_flag = 0;
$ula9_flag = 0;
$ula10_flag = 0;
$ula11_flag = 0;
	while( ($BIB_ID[$i], $AUTHOR[$i], $TITLE[$i], $EDITION[$i], $REQPUBLISHER[$i], $REQPUBDATE[$i], $REQISBN[$i], $REQISSN[$i], $REQPUBPLACE[$i], $LOCATION_ID[$i], $DISPLAY_CALL_NO[$i], $LOCATION_CODE[$i]) = $dd->fetchrow){
		SWITCH: {
			if( $LOCATION_CODE[$i] =~ /^yr/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^ar/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^ck/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^cl/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^ea/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^er/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
                        if( $LOCATION_CODE[$i] =~ /^mu/ ){ $ROTA_Loc[$i]="ULA7"; $ROTA_CODENUM[$i]=1; $ula7_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^bi/ ){ $ROTA_Loc[$i]="ULA8"; $ROTA_CODENUM[$i]=2; $ula8_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^sg/ ){ $ROTA_Loc[$i]="ULA8"; $ROTA_CODENUM[$i]=2; $ula8_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^sc/ ){ $ROTA_Loc[$i]="ULA8"; $ROTA_CODENUM[$i]=2; $ula8_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^se/ ){ $ROTA_Loc[$i]="ULA8"; $ROTA_CODENUM[$i]=2; $ula8_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^lw/ ){ $ROTA_Loc[$i]="ULA9"; $ROTA_CODENUM[$i]=3; $ula9_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^mg/ ){ $ROTA_Loc[$i]="ULA10"; $ROTA_CODENUM[$i]=4; $ula10_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^aisr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^arbtsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^arscrsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^arsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^bisr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^casr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^clsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^cssr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^easr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^ilisr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^lwsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^mgsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^musr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^scsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^sgsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^smsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^sr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^uesr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^yrncrcsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^yrsr/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			if( $LOCATION_CODE[$i] =~ /^ma/ ){ $ROTA_Loc[$i]="ULA11"; $ROTA_CODENUM[$i]=5; $ula11_flag = 1; last SWITCH; }
			$LOCATION_CODE[$i]="COULD NOT FIND LOCATION_CODE";
			$ROTA_Loc[$i]="COULD NOT SET ROTA_LOC";
			$ROTA_CODENUM[$i]="COULD NOT SET ROTA_CODENUM";
			}

# test
#print"$BIB_ID[$i], $AUTHOR[$i], $TITLE[$i], $EDITION[$i], $REQPUBLISHER[$i], $REQPUBDATE[$i], $REQISBN[$i], $REQISSN[$i], $REQPUBPLACE[$i], $LOCATION_ID[$i], $DISPLAY_CALL_NO[$i], $LOCATION_CODE[$i]";

		$i+=1;
		}
	$numorow = $i-1;

#print"numorow = $numorow\n\n";

###################################################################################
# one email per record
$message="";

	chomp( $rfile1_split[10] );
	$rfile1_split[10] =~ s/  //g;

# temp to keep interpreter happy while in development
#$message = "$BIB_ID[$i], $AUTHOR[$i], $TITLE[$i], $EDITION[$i], $REQPUBLISHER[$i], $REQPUBDATE[$i], $REQISBN[$i], $REQISSN[$i], $REQPUBPLACE[$i], $LOCATION_ID[$i], $DISPLAY_CALL_NO[$i], $LOCATION_CODE[$i]";


######### dhc note: see bottom of this file for the old $message's. Delete when done.
$PickupLocation =~ s/WDDS_Web_PU at YRL/Web_PU at YRL/ig ;
$PickupLocation =~ s/WDDS_Web_PU at Biomed/Web_PU at Biomed/ig ;
$PickupLocation =~ s/WDDS_Web_PU at Law/Web_PU at Law/ig ;
$PickupLocation =~ s/WDDS_Web_PU at AGSM/Web_PU at AGSM/ig ;
$PickupLocation =~ s/WDDS_Web_PU at SEL_Chem/Web_PU at SEL_Chem/ig ;
$PickupLocation =~ s/WDDS_Web_PU at SEL_EMS/Web_PU at SEL_EMS/ig ;
$PickupLocation =~ s/WDDS_Web/Post on Web/ig ;
$PickupLocation =~ s/WDDS_Web_MailOnCampus/Web_Mail On Campus/ig ;
$PickupLocation =~ s/WDDS_Web_MailOffCampus/Web_Mail Off Campus/ig ;
$PickupLocation =~ s/WDDS_Pickup at Biomed/Pickup at Biomed/ig ;
$PickupLocation =~ s/WDDS_Pickup at Law/Pickup at Law/ig ;
$PickupLocation =~ s/WDDS_Pickup at YRL/Pickup at YRL/ig ;
$PickupLocation =~ s/WDDS_Mail off campus/Mail off campus/ig ;
$PickupLocation =~ s/WDDS_Mail on campus/Mail on campus/ig ;
$ClientLocation =~ s/YDDSHOME_YRL/ULA7/ig ;
$ClientLocation =~ s/YDDSHOME_Biomed/ULA8/ig ;
$ClientLocation =~ s/YDDSHOME_Law/ULA9/ig ;
$ClientLocation =~ s/YDDSHOME_Man/ULA10/ig ;
$ClientLocation =~ s/YDDSHOME_SRLF/ULA11/ig ;
unless ($ClientLocation) {$ClientLocation = "ULA7";}
	$message = "
ReqSymbol=$ClientLocation
ReqVerifySource=DDS Formatted Email
USERID=$patron_id
ClientLocation=$ClientLocation
ClientLastName=$rfile1_split[14], $rfile1_split[15]
ClientCategory=$rfile1_split[18]
ClientAddr4Street=$ClientAddr4Street
ClientAddr4City=$ClientAddr4City
ClientAddr4Region=$ClientAddr4Region
ClientAddr4Code=$ClientAddr4Code
ClientAddr4Phone=$ClientAddr4Phone
ClientEmailAddress=$ClientEmailAddress
borupb=$rfile1_split[18]
Notes=$PickupLocation Note: $rfile1_split[19]
PickupLocation=$ClientLocation
ReqMediaType1=$ClientLocation2
WillPayFee=Y
PatronKey=MELVYLVDX";
if( $rcnt == 0 ){
	$message .= "
MaterialType=MONO
ServiceTp1=1
ServiceTp2=2
RequestMediaType1=1";}
elsif( $rcnt == 1 ){
	$message .= "
MaterialType=SERAL
ServiceTp1=2
ServiceTp2=1
ReqDeliveryMethod=ILL-DM1
RequestMediaType1=6";}
	$message .= "
ReqAuthor=$rfile1_split[11]
ReqArticleAuthor=$rfile1_split[2]
ReqTitle=$rfile1_split[10]
ReqArticleTitle=$rfile1_split[3]
ReqPubPlace=$REQPUBPLACE[1]
ReqPublisher=$REQPUBLISHER[1]
ReqEdition=$rfile1_split[12]
ReqPubDate=$REQPUBDATE[1]
ReqPartPubDate=$rfile1_split[6]
ReqPagination=$rfile1_split[5]
ReqISBN=$REQISBN[1]
ReqISSN=$REQISSN[1]
ReqIssueTitle=$rfile1_split[4]
";



#print the ROTA stuff
	$firstpass1 = 0;
	$instance = 1;
	for( $jj=1; $jj<=$numorow; $jj++ ){
		if( $ROTA_CODENUM[$jj] == 1 ){
			if( $firstpass1 == 0 ){
				$Rota_new1="$instance";
				$RotaLoc1="$ROTA_Loc[$jj]";
				$RotaCallNumber1="$LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
				$RotaSource1="L";
				$RotaDefaultNamingAuthority1="Melvyl";
				$firstpass1 = 1;
				}
			else{
				$RotaCallNumber1="${RotaCallNumber1}; $LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
			}
			}
		}


	$firstpass2 = 0;
	$instance = 2;
	for( $jj=1; $jj<=$numorow; $jj++ ){
		if( $ROTA_CODENUM[$jj] == 2 ){
			if( $firstpass2 == 0 ){
				$Rota_new2="$instance";
				$RotaLoc2="$ROTA_Loc[$jj]";
				$RotaCallNumber2="$LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
				$RotaSource2="L";
				$RotaDefaultNamingAuthority2="Melvyl";
				$firstpass2 = 1;
				}
			else{
				$RotaCallNumber2="${RotaCallNumber2}; $LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
			}
			}
		}


	$firstpass3 = 0;
	$instance = 3;
	for( $jj=1; $jj<=$numorow; $jj++ ){
		if( $ROTA_CODENUM[$jj] == 3 ){
			if( $firstpass3 == 0 ){
				$Rota_new3="$instance";
				$RotaLoc3="$ROTA_Loc[$jj]";
				$RotaCallNumber3="$LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
				$RotaSource3="L";
				$RotaDefaultNamingAuthority3="Melvyl";
				$firstpass3 = 1;
				}
			else{
				$RotaCallNumber3="${RotaCallNumber3}; $LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
			}
			}
		}


	$firstpass4 = 0;
	$instance = 4;
	for( $jj=1; $jj<=$numorow; $jj++ ){
		if( $ROTA_CODENUM[$jj] == 4 ){
			if( $firstpass4 == 0 ){
				$Rota_new4="$instance";
				$RotaLoc4="$ROTA_Loc[$jj]";
				$RotaCallNumber4="$LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
				$RotaSource4="L";
				$RotaDefaultNamingAuthority4="Melvyl";
				$firstpass4 = 1;
				}
			else{
				$RotaCallNumber4="${RotaCallNumber4}; $LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
			}
			}
		}


	$firstpass5 = 0;
	$instance = 5;
	for( $jj=1; $jj<=$numorow; $jj++ ){
		if( $ROTA_CODENUM[$jj] == 5 ){
			if( $firstpass5 == 0 ){
				$Rota_new5="$instance";
				$RotaLoc5="$ROTA_Loc[$jj]";
				$RotaCallNumber5="$LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
				$RotaSource5="L";
				$RotaDefaultNamingAuthority5="Melvyl";
				$firstpass5 = 1;
				}
			else{
				$RotaCallNumber5="${RotaCallNumber5}; $LOCATION_CODE[$jj] $DISPLAY_CALL_NO[$jj]";
			}
			}
		}

$Rota_new = 1;
if( $firstpass1 == 1 ){
$message .= "Rota._new=$Rota_new
Rota.Loc=$RotaLoc1
Rota.CallNumber=$RotaCallNumber1
Rota.Source=$RotaSource1
Rota.DefaultNamingAuthority=$RotaDefaultNamingAuthority1
";
$Rota_new += 1;
}

if( $firstpass2 == 1 ){
$message .= "Rota._new=$Rota_new
Rota.Loc=$RotaLoc2
Rota.CallNumber=$RotaCallNumber2
Rota.Source=$RotaSource2
Rota.DefaultNamingAuthority=$RotaDefaultNamingAuthority2
";
$Rota_new += 1;
}

if( $firstpass3 == 1 ){
$message .= "Rota._new=$Rota_new
Rota.Loc=$RotaLoc3
Rota.CallNumber=$RotaCallNumber3
Rota.Source=$RotaSource3
Rota.DefaultNamingAuthority=$RotaDefaultNamingAuthority3
";
$Rota_new += 1;
}

if( $firstpass4 == 1 ){
$message .= "Rota._new=$Rota_new
Rota.Loc=$RotaLoc4
Rota.CallNumber=$RotaCallNumber4
Rota.Source=$RotaSource4
Rota.DefaultNamingAuthority=$RotaDefaultNamingAuthority4
";
$Rota_new += 1;
}

if( $firstpass5 == 1 ){
$message .= "Rota._new=$Rota_new
Rota.Loc=$RotaLoc5
Rota.CallNumber=$RotaCallNumber5
Rota.Source=$RotaSource5
Rota.DefaultNamingAuthority=$RotaDefaultNamingAuthority5
";
$Rota_new += 1;
}





$mailx = '/usr/bin/mailx';
open MAILX, "|$mailx -s '$subject' -c '$cc_email' $t_email" or die "Can not run $mailx $!\n";
	print MAILX "$message";
	close MAILX;
	}
}

}


if( $db_flag == 1 ){
	$dd->finish();
	}

#### auto named transaction file 5 (uncomment to re-activate)
close( TRANS );

## dhc temp disable
###rename source files
$time = time;
for( $i=0; $i<2; $i++ ){
	if( -e $rfile[$i] ){
		$new_rfile = $rfile[$i].".".$time;
		`mv $rfile[$i] $new_rfile`;
		}
	}

#### auto named transaction file 6 (uncomment to re-activate)
$transtime = $trans.".".$time;
`mv $trans $transtime`;

exit(0);
