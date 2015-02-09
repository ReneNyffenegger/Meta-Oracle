use warnings;
use strict;
use utf8;

use Win32::LongPath;

use File::Touch;

use DBI qw(:sql_types);

my $dbh = DBI -> connect("dbi:Oracle:", 'meta', 'meta') or die;

#my $root_dir = "\\\\?\\c:\\github\\Meta-Oracle";
#my $root_dir = "c:\\github\\Meta-Oracle";

create_directories();

error_messages();
dynamic_performance_views();

sub error_messages { # {{{

    open (my $readme, '>', '../errors/README.md') or die; 

    print $readme "<table>\n";

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

       my $dir = "..\\errors\\$filename_friendly_error";
#      mkdir_with_gitignore("..\\errors\\$filename_friendly_error");
       mkdir_($dir);

       openL (\my $readme_, '>', "$dir\\README.md") or die "$dir\\README.md";
       print $readme_ "# $error\n";
       close $readme_;


       print $readme "<tr>";
       
       print $readme "<td><a href='https://github.com/ReneNyffenegger/Meta-Oracle/tree/master/errors/$filename_friendly_error'>`$error`</a><td>";

       print $readme "<tr>\n";
    }
    
} # }}}


sub dynamic_performance_views { # {{{


    open (my $readme, '>', '../dynamic-performance-views/README.md') or die; 

    print $readme "<table><tr><td><b>View Name</b></td><td>GV View?</td></tr>\n";

    my $sth = $dbh -> prepare(q{
    
    select
      lower(v.view_name)         view_name,
      nvl2(gv.view_name, 1, 0)   corresponding_gv_view
    from
      v$fixed_view_definition  v left join
      v$fixed_view_definition gv on 
                      substr( v.view_name, 3, length( v.view_name)-2) = 
                      substr(gv.view_name, 4, length(gv.view_name)-3) and
                      substr(gv.view_name, 1, 3) = 'GV$'
    where
                      substr( v.view_name, 1, 2) =  'V$'
    order by 1

    }) or die;

    $sth -> execute;

    while (my $r = $sth -> fetchrow_hashref) {

#     print $r -> {VIEW_NAME}, "\t", $r -> {CORRESPONDING_GV_VIEW}, "\n";

      my $view_name = $r->{VIEW_NAME};

      my $dir = "..\\dynamic-performance-views\\$view_name";
#     mkdir_with_gitignore("..\\dynamic-performance-views\\$view_name");
      mkdir_($dir);

      openL (\my $readme_, '>', "$dir\\README.md") or die "$dir\\README.md";
      print $readme_ "# $view_name\n";
      close $readme_;

      print $readme "<tr>";
      
      print $readme "<td><a href='https://github.com/ReneNyffenegger/Meta-Oracle/tree/master/dynamic-performance_views/$view_name'>`$view_name`</a><td>";

      if ($r->{CORRESPONDING_GV_VIEW}) {
         print $readme '<td style="color:blue">Yes</td></tr>';
      }
      else {
         print $readme '<td style="color:green">No</td></tr>';
      }

      print $readme "<tr>\n";
 
    }

    print $readme "</table>";
    close $readme;
    
} # }}}


sub filename_friendly_name { # {{{
    my $filename = shift;

    $filename =~ s/\s+/-/g;
    $filename =~ s/:/_/g;
    $filename =~ s/\?\\|\///g;
    $filename =~ s/"/«/;
    $filename =~ s/"/»/;
#   $filename =~ s/"//;
    $filename =~ s/\\//g;

    $filename =~ s/"//g;
    $filename =~ s/\*//g;
    $filename =~ s/\|//g;
    $filename =~ s/>/GT/g;
    $filename =~ s/</LT/g;
    $filename =~ s/\.$//g;
    $filename =~ s/\?//g;

    $filename =~ s/-$//;

    if ($^O eq 'MSWin32') { # or MSWin64 ?
      return substr($filename, 0, 230);
    }

    return $filename;

} # }}}


sub create_directories { # {{{

    mkdir_ ('..\errors'); 
    mkdir_ ('..\dynamic-performance-views'); 
    
} # }}}


sub mkdir_with_gitignore { # {{{

    my $dirname = shift;

    mkdir_ ($dirname);

    openL (\my $x, '>', "$dirname\\.gitignore") or die "$dirname\\.gitignore";
    close $x;

#   touch("$dirname\\.gitignore");
    
} # }}}

sub mkdir_ { # {{{
    my $dir = shift;
    if ($^O eq 'MSWin32') { # or MSWin64?
      mkdirL $dir
    }
    else {
      mkdir  $dir;
    }
} # }}}
