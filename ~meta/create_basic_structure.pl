use warnings;
use strict;

use DBI qw(:sql_types);

my $dbh = DBI -> connect("dbi:Oracle:", 'meta', 'meta') or die;

create_directories();

error_messages();

sub error_messages { # {{{

    my $sth = $dbh -> prepare ("
      begin
        :error := sqlerrm(:i);
      end;"
    );

    for my $i (0 .. 100000) {
       $sth -> bind_param      (':i'    ,    -$i          ); 
       $sth -> bind_param_inout(':error', \my $error, 1000);

       $sth -> execute;

       next unless defined $error;
       next if $error =~ /ORA-\d\d\d\d\d:\s*$/;
       next if $error =~ /non-ORACLE exception/;
       next if $error =~ /product=RDBMS; facility=ORA/;

       $error =~ s/ORA-0000:/ORA-00000:/ if $i == 0;

       print $error, "<\n" if $i == 20490;

       my $filename_friendly_error = $error;
       $filename_friendly_error =~ s/: /_/;

       $filename_friendly_error = filename_friendly_name($filename_friendly_error);

       mkdir "../errors/$filename_friendly_error";
    }
    
} # }}}


sub filename_friendly_name { # {{{
    my $filename = shift;

    $filename =~ s/\s+/-/g;
    $filename =~ s/:/_/g;
    $filename =~ s/\?\\|\///g;

    $filename =~ s/-$//;

    return $filename;

} # }}}


sub create_directories { # {{{

    mkdir '../errors'; 
    
} # }}}
