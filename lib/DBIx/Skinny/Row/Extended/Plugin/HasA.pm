package  DBIx::Skinny::Row::Extended::Plugin::HasA;
use strict;
use warnings;
use UNIVERSAL::require;
use String::CamelCase qw();

sub import {
    my $pkg = caller;
    {
        no strict 'refs'; ## no critic
        *{"$pkg\::mk_has_a_accessor"} = \&mk_has_a_accessor;
    }
}

sub mk_has_a_accessor {
    my ($class, $colname, $row_class) = @_;

    $class =~ m/^(.+::Row)::(.+)$/;
    $row_class = $1 . $row_class;
    $row_class->use
        or die $@;
    my $method_name = $row_class->table_name;

    my $code = sub {
        my $self = shift;

        if ( $_[0] ) {
            $self->{"__$method_name"} = $_[0];
        } else {
            if ( $self->{"__$method_name"} ) {
                return $self->{"__$method_name"};
            } else {
                return $self->{"__$method_name"} = do {
                    $row_class->single({ id => $self->$colname });
                };
            }
        }
    };

    {
        no strict 'refs'; ## no critic
        *{"$class\::$method_name"} = $code;
    }
}

1;
