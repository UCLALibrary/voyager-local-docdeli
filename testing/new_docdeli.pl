#!/m1/shared/bin/perl
# Document delivery data parsing script:
## Parse Voyager-generated structured data files
## Get extra data from Voyager database
## Map data to fields needed for VDX document delivery system
## Create structured email message
## Send email to CDL for VDX processing

use strict;
use DBI;
use Data::Dumper qw(Dumper);

# This script handles two types of requests, which Voyager creates in two separate files.
# File format is identical, so same code can handle both.
## OE_LN: Document Delivery - Book Request (fee)
## OE_PH: Document Delivery - Copy Request (fee)
## /m1/voyager/ucladb/rpt/opacrequests.OE_LN.out
## /m1/voyager/ucladb/rpt/opacrequests.OE_PH.out

# 1 required argument
if ($#ARGV != 0) {
  print "\nUsage: $0 opacrequest_file\n";
  exit 1;
}

my $opacrequest_file = $ARGV[0];

ProcessFile($opacrequest_file);

# End of main script; real work done by subroutines below.
exit 0;

##############################
sub ProcessFile {
  my $input_file = shift;
  my @lines = ReadFile($input_file);

  # Iterate over lines, turning each into an email message
  foreach my $line (@lines) {
    ###DumpLine($line);  ### DEBUGGING
    my %data = ParseLine($line);
    %data = AddVoyagerData(%data);
    DumpData(%data); ### DEBUGGING
    FormatForEmail(%data);
    print "\n";
  }
}

##############################
sub ReadFile {
  my $input_file = shift;
  # File often won't exist when this script is called, so continue only if it has content.
  return if (! -s $input_file);
  open(INFILE, $input_file) || die "Could not open $input_file for reading\n";
  my @raw_lines = <INFILE>;
  close INFILE;

  my $raw_line_count = scalar(@raw_lines);
  print "Processing $input_file : $raw_line_count lines...\n";

  # Concatenate everything into one line for convoluted LF fixing.
  # Correct input is one+ records, each with 21 fields, delimited by '|'
  # 0|1|...|20\n0|1|...|20\n etc.
  # Some fields other than #20 may have line breaks - incorrectly - but
  # we only care about the one at the end of the final field of each record.
  my $one_long_line = '';
  foreach(@raw_lines) {
    $one_long_line = $one_long_line.$_;
  }

  # First clean up known data problems
  $one_long_line =~ s/\x0b/ /g;		# ^k - VT - 013 octal - 0b hex
  $one_long_line =~ s/\s\s//g;		# Strip repeated spaces

  # Find the good line break (\n) at end of each record:
  # e.g., replace \n with $temp_record_sep in |1234\n5678|
  # (numerical field at end of record, no delimiter, line break, numerical field at start of next record).
  my $temp_record_sep = 's67sd67sd7';	# arbitrary, fake data
  $one_long_line =~ s/(\|[0-9]+)\n([0-9]+\|)/\1$temp_record_sep\2/g;

  # Now remove any remaining line breaks
  $one_long_line =~ s/\R//g;		# Remove line breaks

  # Finally, re-split on the temporary line ending to get the clean data for further processing.
  my @clean_lines = split($temp_record_sep, $one_long_line);

  # Print a message if the above has reduced the line count.
  my $clean_line_count = scalar(@clean_lines);
  if ($clean_line_count != $raw_line_count) {
    print "WARNING: Line count has changed from $raw_line_count to $clean_line_count\n";
  }

  return @clean_lines;
}

##############################
sub DumpLine {
  # Used for debugging
  my $line = shift;
  print "$line\n";
  my @fields = split('\|', $line);
  my $field_num = 0;
  foreach(@fields) {
    print "$field_num\t$_\n";
    $field_num++;
  }
}

##############################
sub DumpData {
  # Used for debugging
  my %data = @_;	# Hash, can't just use shift
  my $count = scalar keys %data;
  print "Size of data: $count\n";
  for my $key (sort keys %data) {
    my $reftype = ref($data{$key});
    if ($reftype eq 'ARRAY') {
      print "$key:\n";
      my @array = @{$data{$key}};
      foreach my $arrayval (@array) {
        DumpSpecial("\t", $arrayval);
      }
    }
    else {
      print "$key:\t$data{$key}\n";
    }
  }
}

##############################
sub DumpSpecial {
  # Used for debugging
  my ($indent, $val) = @_;
  my $reftype = ref($val);
  if ($reftype eq 'ARRAY') {
    print "ARRAY!\n";
  }
  elsif ($reftype eq 'HASH') {
    foreach my $key (sort keys %$val) {
      print "$indent" . "$key:\t${$val}{$key}\n";
    }
  }
  else {
    print "OTERH!\n";
  }
}

##############################
sub ParseLine {
  # Splits delimited line and stores each piece in hash / associative array.
  my $line = shift;
  my @fields = split('\|', $line);

  # See https://docs.library.ucla.edu/x/ZoGI for documentation of file format.
  my %data = (
    'request_id'	=> $fields[0],
    'request_dt'	=> $fields[1],
    'user_field1'	=> $fields[2],
    'user_field2'	=> $fields[3],
    'user_field3'	=> $fields[4],
    'user_field4'	=> $fields[5],
    'user_field5'	=> $fields[6],
    'user_field6'	=> $fields[7],
    'bib_id'		=> $fields[8],
    'title_brief'	=> $fields[9],
    'title_full'	=> $fields[10],
    'author'		=> $fields[11],
    'edition'		=> $fields[12],
    'mfhd_id'		=> $fields[13],
    'patron_last_name'	=> $fields[14],
    'patron_first_name'	=> $fields[15],
    'patron_barcode'	=> $fields[16],
    'patron_group_name'	=> $fields[17],
    'patron_group_code'	=> $fields[18],
    'user_comment'	=> $fields[19],
    'patron_id'		=> $fields[20],
  );

  # Translate some data for VDX use, keeping original Voyager data for comparison/debugging
  $data{'client_category'} = TranslatePatronGroup($data{'patron_group_code'});
  return %data;
}

##############################
sub AddVoyagerData {
  my %data = @_;	# Hash, can't just use shift
  %data = GetVoyagerBibMfhdData(%data);
  %data = GetVoyagerPatronData(%data);
  return %data;
}

##############################
sub GetVoyagerBibMfhdData {
  my %data = @_;	# Hash, can't just use shift
  my @rotas = ();	# Array for holdings-level rota data
  my $bib_id = $data{'bib_id'};
  my $sql = BuildBibMfhdSQL($bib_id);
  my $dbh = GetDBConnection();
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  while (my $row = $sth->fetchrow_hashref) {
    # Bib-level data - the same across multiple rows, but not worth special handling here.
    $data{'isbn'} = $row->{'ISBN'};
    $data{'issn'} = $row->{'ISSN'};
    $data{'pub_place'} = $row->{'PUB_PLACE'};
    $data{'publisher'} = $row->{'PUBLISHER'};
    $data{'pub_date'} = $row->{'PUBLISHER_DATE'};
    # Holdings-level data - different in each row
    my $location_code = $row->{'LOCATION_CODE'};
    my ($rota_priority, $ula_value) = GetRotaData($location_code);
    my %rota = (
      'call_number' => $row->{'DISPLAY_CALL_NO'},
      'location_code' => $location_code,
      'rota_priority' => $rota_priority,
      'ula_value' => $ula_value,
    );
    # Add hashref to array
    push(@rotas, \%rota);
  }
  # Add completed rotas array to overall data hash
  $data{'rotas'} = [@rotas];
  $sth->finish();
  $dbh->disconnect();
  return %data;
}

##############################
sub GetVoyagerPatronData {
  my %data = @_;	# Hash, can't just use shift
  my $patron_id = $data{'patron_id'};
  my $sql = BuildPatronSQL($patron_id);
  my $dbh = GetDBConnection();
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  my $dbdata = $sth->fetchrow_hashref;
  #####$data{'city'} = ${$dbdata}{'CITY'};
  $data{'street_address'}	= $dbdata->{'STREET_ADDRESS'};
  $data{'city'}			= $dbdata->{'CITY'};
  $data{'state_province'}	= $dbdata->{'STATE_PROVINCE'};
  $data{'zip_postal'}		= $dbdata->{'ZIP_POSTAL'};
  $data{'phone_number'}		= $dbdata->{'PHONE_NUMBER'};
  $data{'email_address'}	= $dbdata->{'EMAIL_ADDRESS'};
  $data{'client_location'}	= $dbdata->{'CLIENT_LOCATION'};
  $data{'pickup_location'}	= $dbdata->{'PICKUP_LOCATION'};
  $sth->finish();
  $dbh->disconnect();

  return %data;
}

##############################
sub BuildBibMfhdSQL {
  # Returns SQL for the given bib and non-suppressed holdings data
  my $bib_id = shift;
  return ("
  select distinct
    bt.bib_id
  , bt.publisher
  , bt.pub_place
  , bt.publisher_date
  , bt.isbn
  , bt.issn
  , mm.display_call_no
  , l.location_code
  from bib_text bt
  inner join bib_mfhd bm on bt.bib_id = bm.bib_id
  inner join mfhd_master mm on bm.mfhd_id = mm.mfhd_id
  inner join location l on mm.location_id = l.location_id
  where bt.bib_id = $bib_id
  and mm.suppress_in_opac = 'N'
  and l.suppress_in_opac = 'N'
  ");
}

##############################
sub BuildPatronSQL {
  my $patron_id = shift;
  return ("
  select
    p.patron_id
  , p.last_name
  , p.first_name
  , pa.address_line1 as street_address
  , pa.city
  , pa.state_province
  , pa.zip_postal
  , ( select phone_number
      from patron_phone pp
      inner join phone_type pt on pp.phone_type = pt.phone_type
      where address_id = pa.address_id
      and pt.phone_desc = 'Primary'
      and rownum < 2 -- just in case
    ) as phone_number
  , ( select pa2.address_line1
      from patron_address pa2
      inner join address_type adt2 on pa2.address_type = adt2.address_type
      where pa2.patron_id = p.patron_id
      and adt2.address_desc = 'EMail'
    ) as email_address
  , ( select psc1.patron_stat_desc
      from patron_stats ps1
      inner join patron_stat_code psc1 on ps1.patron_stat_id = psc1.patron_stat_id
      where ps1.patron_id = p.patron_id
      and psc1.patron_stat_desc like 'YDDS%'
      and rownum < 2
    ) as client_location
  , ( select psc2.patron_stat_desc
      from patron_stats ps2
      inner join patron_stat_code psc2 on ps2.patron_stat_id = psc2.patron_stat_id
      where ps2.patron_id = p.patron_id
      and psc2.patron_stat_desc like 'WDDS%'
      and rownum < 2
    ) as pickup_location
  from patron p
  inner join patron_address pa on p.patron_id = pa.patron_id
  inner join address_type adt on pa.address_type = adt.address_type
  where adt.address_desc = 'Permanent'
  and p.patron_id = $patron_id
  ");
}

##############################
sub GetRotaData {
  # Parameter: Voyager location code
  # Returns: array of rota priority and ULA value
  my $loc = shift;
  my $rota_priority;
  my $ula_value;

  # Fake loop since no supported case/switch in perl...
  for ($loc) {
    # Humanities (generally) locations
    if    (/^ar/) {$rota_priority = 1; $ula_value = 'ULA7';}
    elsif (/^ck/) {$rota_priority = 1; $ula_value = 'ULA7';}
    elsif (/^cl/) {$rota_priority = 1; $ula_value = 'ULA7';}
    elsif (/^ea/) {$rota_priority = 1; $ula_value = 'ULA7';}
    elsif (/^er/) {$rota_priority = 1; $ula_value = 'ULA7';}
    elsif (/^mu/) {$rota_priority = 1; $ula_value = 'ULA7';}
    elsif (/^yr/) {$rota_priority = 1; $ula_value = 'ULA7';}
    # Sciences locations
    elsif (/^bi/) {$rota_priority = 2; $ula_value = 'ULA8';}
    elsif (/^sc/) {$rota_priority = 2; $ula_value = 'ULA8';}
    elsif (/^se/) {$rota_priority = 2; $ula_value = 'ULA8';}
    elsif (/^sg/) {$rota_priority = 2; $ula_value = 'ULA8';}
    elsif (/^sm/) {$rota_priority = 2; $ula_value = 'ULA8';}
    # Law locations
    elsif (/^lw/) {$rota_priority = 3; $ula_value = 'ULA9';}
    # Management locations
    elsif (/^mg/) {$rota_priority = 4; $ula_value = 'ULA10';}
    # SRLF locations
    elsif (/^sr/) {$rota_priority = 5; $ula_value = 'ULA11';}
    # Default values if Voyager location does not match
    else {
      $rota_priority = 9999;	# high value = lowest priority
      $ula_value = "COULD NOT FIND LOCATION CODE: $loc";
    }
  }

  return ($rota_priority, $ula_value);
}

##############################
sub TranslatePatronGroup {
  # Parameter: Voyager patron group code
  # Returns: Client category Value needed for VDX
  my $patron_group_code = shift;
  my $vdx_category;
  
  # Fake loop since no supported case/switch in perl...
  for ($patron_group_code) {
    # Faculty
    if    (/^UADD/)   {$vdx_category = 'FACUL';}
    elsif (/^UALAD/)  {$vdx_category = 'FACUL';}
    elsif (/^UANLDD/) {$vdx_category = 'FACUL';}
    elsif (/^UAPDD/)  {$vdx_category = 'FACUL';}
    # Graduate students
    elsif (/^UGDD/)   {$vdx_category = 'GRADU';}
    elsif (/^UGLDD/)  {$vdx_category = 'GRADU';}
    elsif (/^UGMDD/)  {$vdx_category = 'GRADU';}
    elsif (/^UGNDD/)  {$vdx_category = 'GRADU';}
    # Undergraduate students
    elsif (/^UUDD/)   {$vdx_category = 'UNDRG';}
    # General patrons
    elsif (/^USDD/)   {$vdx_category = 'PATRN';}
    elsif (/^USLDD/)  {$vdx_category = 'PATRN';}
    # Unexpected patron group
    else {$vdx_category = "UNKNOWN: $patron_group_code";}
  }

  return $vdx_category;
}

##############################
sub FormatForEmail {
  my %data = @_;	# Hash, can't just use shift

  print "\n ===== Data for email message =====\n"; ### DEBUGGING
  # I don't like HEREDOC/qq indentation workarounds in perl; message needs to be strictly formatted
  my $message = "";
  $message .= "ReqSymbol=TBD\n";
  $message .= "ReqVerifySource=DDS Formatted Email\n";
  $message .= "USERID=$data{'patron_barcode'}\n";
  $message .= "ClientLocation=TBD\n";
  $message .= "ClientLastName=$data{'patron_last_name'}, $data{'patron_first_name'}\n";	# Yes: actually last, first...
  $message .= "ClientCategory=$data{'client_category'}\n";
  $message .= "ClientAddr4Street=$data{'street_address'}\n";
  $message .= "ClientAddr4City=$data{'city'}\n";
  $message .= "ClientAddr4Region=$data{'state_province'}\n";
  $message .= "ClientAddr4Code=$data{'zip_postal'}\n";
  $message .= "ClientAddr4Phone=$data{'phone_number'}\n";
  $message .= "ClientEmailAddress=$data{'email_address'}\n";
  $message .= "borupb=$data{'client_category'}\n"; # Same as ClientCategory above
  $message .= "Notes=TBD\n";
  $message .= "PickupLocation=TBD\n";
  $message .= "ReqMediaType1=TBD\n";
  $message .= "WillPayFee=Y\n";
  $message .= "PatronKey=MELVYLVDX\n";
  # Start conditional MONO vs. SERAL [sic]
  $message .= "MaterialType=TBD\n";
  $message .= "ServiceTp1=TBD\n";
  $message .= "ServiceTp2=TBD\n";
  $message .= "ReqDeliveryMethod=ILL-DM1\n"; ### This line added only for SERAL [sic]
  $message .= "RequestMediaType1=TBD\n";
  # End conditional MONO vs. SERAL [sic]
  $message .= "ReqAuthor=$data{'author'}\n";
  $message .= "ReqArticleAuthor=TBD\n";
  $message .= "ReqTitle=$data{'title_full'}\n";
  $message .= "ReqArticleTitle=TBD\n";
  $message .= "ReqPubPlace=$data{'pub_place'}\n";
  $message .= "ReqPublisher=$data{'publisher'}\n";
  $message .= "ReqEdition=$data{'edition'}\n";
  $message .= "ReqPubDate=$data{'pub_date'}\n";
  $message .= "ReqPartPubDate=TBD\n";
  $message .= "ReqPagination=TBD\n";
  $message .= "ReqISBN=$data{'isbn'}\n";
  $message .= "ReqISSN=$data{'issn'}\n";
  $message .= "ReqIssueTitle=TBD\n";
  # TODO: Rota output

  print "$message"; ### DEBUGGING

}

##############################
##############################
##############################
##############################
##############################
##############################
##############################
sub GetDBConnection {
  my $dsn = "dbi:Oracle:host=localhost;sid=VGER";
  my $schema = 'ucla_preaddb';
  # Requires these files...
  # TODO: Get VGER_SCRIPTS and VGER_CONFIG from environment, via wrapper shell script if needed.
  chomp (my $passwd = `/usr/local/bin/voyager/scripts/get_value.pl /usr/local/bin/voyager/config/vger_db_credentials $schema`);
  return DBI->connect ($dsn, $schema, $passwd);
}

##############################
