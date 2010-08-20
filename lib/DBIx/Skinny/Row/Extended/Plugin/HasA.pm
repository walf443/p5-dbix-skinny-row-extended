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

    $row_class = $class->base_namespace . "::" . $row_class;
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
                    if ( $row_class->can('fetch_multi_by_id') ) {
                        my $row_class_id = $self->$colname;
                        return $row_class->fetch_multi_by_id({ id => [ $row_class_id] })->{$row_class_id};
                    } else {
                        return $row_class->single({ id => $self->$colname });
                    }
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
