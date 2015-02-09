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

#error_messages();
#dynamic_performance_views();
execution_plan_operations();

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

sub execution_plan_operations { # {{{
  execution_plan_operation('and-equal'               , ''                      );
  
  execution_plan_operation('bitmap'                  , 'index'                 );
  execution_plan_operation('bitmap'                  , 'merge'                 );
  execution_plan_operation('bitmap'                  , 'minus'                 );
  execution_plan_operation('bitmap'                  , 'or'                    );
  execution_plan_operation('bitmap'                  , 'and'                   );
  execution_plan_operation('bitmap'                  , 'key iteration'         );
  
  execution_plan_operation('connect by'              , ''                      );
  execution_plan_operation('concatenation'           , ''                      );
  
  execution_plan_operation('count'                   , ''                      );
  execution_plan_operation('count'                   , 'stopkey'               );
  
  execution_plan_operation('cube scan'               , ''                      );
  execution_plan_operation('cube scan'               , 'partion outer'         );
  execution_plan_operation('cube scan'               , 'outer'                 );
  
  execution_plan_operation('domain index'            , ''                      );
  
  execution_plan_operation('filter'                  , ''                      );
  execution_plan_operation('first row'               , ''                      );
  execution_plan_operation('for update'              , ''                      );
  
  execution_plan_operation('hash'                    , 'group by'              );
  execution_plan_operation('hash'                    , 'group by pivot'        );
  
  # Join operations
  execution_plan_operation('hash join'               , ''                      );
  execution_plan_operation('hash join'               , 'anti'                  );
  execution_plan_operation('hash join'               , 'semi'                  );
  execution_plan_operation('hash join'               , 'right anti'            );
  execution_plan_operation('hash join'               , 'right semi'            );
  execution_plan_operation('hash join'               , 'outer'                 );
  execution_plan_operation('hash join'               , 'right outer'           );
  
  # Access methods:
  execution_plan_operation('index'                   , 'unique scan'           );
  execution_plan_operation('index'                   , 'range scan'            );
  execution_plan_operation('index'                   , 'range scan descending' );
  execution_plan_operation('index'                   , 'full scan'             );
  execution_plan_operation('index'                   , 'full scan descending'  );
  execution_plan_operation('index'                   , 'fast full scan'        );
  execution_plan_operation('index'                   , 'skip scan'             );
  
  execution_plan_operation('inlist iterator'         , ''                      );
  execution_plan_operation('intersection'            , ''                      );
  
  execution_plan_operation('merge join'              , 'outer'                 );
  execution_plan_operation('merge join'              , 'anti'                  );
  execution_plan_operation('merge join'              , 'semi'                  );
  execution_plan_operation('merge join'              , 'cartesian'             );
  
  # Access methods:
  execution_plan_operation('mat_view rewrite access' , 'full'                  );
  execution_plan_operation('mat_view rewrite access' , 'sample'                );
  execution_plan_operation('mat_view rewrite access' , 'cluster'               );
  execution_plan_operation('mat_view rewrite access' , 'hash'                  );
  execution_plan_operation('mat_view rewrite access' , 'by rowid range'        );
  execution_plan_operation('mat_view rewrite access' , 'sample by rowid range' );
  execution_plan_operation('mat_view rewrite access' , 'by user rowid'         );
  execution_plan_operation('mat_view rewrite access' , 'by index rowid'        );
  execution_plan_operation('mat_view rewrite access' , 'by global index rowid' );
  execution_plan_operation('mat_view rewrite access' , 'by local index rowid'  );
  
  execution_plan_operation('minus'                   , ''                      );
  
  # Join operations
  execution_plan_operation('nested loops'            , ''                      );
  execution_plan_operation('nested loops'            , 'outer'                 );
  
  execution_plan_operation('partition'               , ''                      );
  execution_plan_operation('partition'               , 'single'                );
  execution_plan_operation('partition'               , 'iterator'              );
  execution_plan_operation('partition'               , 'all'                   );
  execution_plan_operation('partition'               , 'inlist'                );
  execution_plan_operation('partition'               , 'invalid'               );
  
  # Does the following really exist?
  execution_plan_operation('px iterator'             , 'block,chunk'           );
  
  execution_plan_operation('px coordinator'          , ''                      );
  execution_plan_operation('px_partition'            , ''                      );
  execution_plan_operation('px receive'              , ''                      );
  execution_plan_operation('px send'                 , 'qc (random),hash,range');
  
  execution_plan_operation('remote'                  , ''                      );
  execution_plan_operation('sequence'                , ''                      );
  
  execution_plan_operation('sort'                    , 'aggregate'             );
  execution_plan_operation('sort'                    , 'unique'                );
  execution_plan_operation('sort'                    , 'group by'              );
  execution_plan_operation('sort'                    , 'group by pivot'        );
  execution_plan_operation('sort'                    , 'join'                  );
  execution_plan_operation('sort'                    , 'order by'              );
  
  # Access methods:
  execution_plan_operation('table access'            , 'full'                  );
  execution_plan_operation('table access'            , 'sample'                );
  execution_plan_operation('table access'            , 'cluster'               );
  execution_plan_operation('table access'            , 'hash'                  );
  execution_plan_operation('table access'            , 'by rowid range'        );
  execution_plan_operation('table access'            , 'sample by rowid range' );
  execution_plan_operation('table access'            , 'by user rowid'         );
  execution_plan_operation('table access'            , 'by index rowid'        );
  execution_plan_operation('table access'            , 'by global index rowid' );
  execution_plan_operation('table access'            , 'by local index rowid'  );
  
  execution_plan_operation('transpose'               , ''                      );
  execution_plan_operation('union'                   , ''                      );
  execution_plan_operation('unpivot'                 , ''                      );
  execution_plan_operation('view'                    , ''                      );
} # }}}

sub execution_plan_operation { # {{{
    my $name_operation = shift;
    my $name_option    = shift;

    my $name_operation_friendly = filename_friendly_name($name_operation);

    if ($name_option) {
      mkdir_("../execution-plan-operations/$name_operation_friendly");
    }
    else {
      mkdir_with_README_md("../execution-plan-operations/$name_operation_friendly", $name_operation);
    }

    if ($name_option) {

      my $name_option_friendly = filename_friendly_name($name_option);

      mkdir_with_README_md("../execution-plan-operations/$name_operation_friendly/$name_option_friendly", "$name_operation - $name_option");

    }

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
    mkdir_ ('..\execution-plan-operations'); 
    
} # }}}

sub mkdir_with_README_md { # {{{

    my $dirname = shift;
    my $title   = shift;

    mkdir_ ($dirname);

    open (my $readme, '>', "$dirname/README.md") or die "$dirname/README.md";
    print $readme "# $title\n" or die;
    close $readme;
    
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
