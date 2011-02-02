package DBIx::Skinny::Row::Extended;
use strict;
use warnings;
use base qw(DBIx::Skinny::Row Class::Data::Inheritable);
use String::CamelCase;

__PACKAGE__->mk_classdata('triggers');

sub table_name {
    my ($class, ) = @_;
    if ( ref $class ) {
        $class = ref $class;
    }
    my $pkg = $class->base_namespace;
    if ( $class =~ m/$pkg\::([^:]+)/ ) {
        my $klass = $1;
        $klass =~ s/::.+$//; # Proj::DB::Row::User::Activeとかのため
        my $result = String::CamelCase::decamelize($1);
        {
            # 一度求めてしまえば後は固定にできるので再定義してしまう
            no strict 'refs'; ## no critic;
            no warnings 'redefine';
            *{"$class\::table_name"} = sub { $result };
        }
        return $result;
    }
}

sub base_namespace {
    die 'please override';
}

sub default_pager_logic { 'PlusOne' }

sub db_master { die 'Please override db_master!' }
sub db_slave  { die 'Please override db_slave!' }

#パラメーターを見てMasterかSlaveか判断するメソッド。Shardingする時などにオーバーライドするとよさげです。
sub get_db {
    my ( $self, $args ) = @_;
    $args ||= {};
    if ( $args->{write} ) {
        return $self->db_master;
    } else {
        return $self->db_slave;
    }
}

sub _search {
    my ($class, $cond, $opt) = @_;

    $opt ||= {};

    unless ( $opt->{no_pager} ) {
        $opt->{page} ||= 1;
        $opt->{limit} ||= 20;
    }

    my ($iter, $pager);
    my $params = {};
    if ( $class->can('condition') ) {
        $params = +{ 
            %{ $class->condition },
            %{ $cond },
        };

    } else {
        $params = $cond;
    }

    my $db = $class->get_db(
        {
            write      => defined $opt->{write} ? $opt->{write} : 0,
            conditions => $cond,
            options    => $opt,
        }
    );
    if ( $opt->{no_pager} ) {
        $iter = $db->search($class->table_name => $params, $opt);
    } else {
        $opt->{pager_logic} ||= $class->default_pager_logic;
        ($iter, $pager) = $db->search_with_pager($class->table_name => $params, $opt);
    }

    return wantarray ? ( $iter, $pager ) : $iter;
}

# 通常の_searchに機能を追加している
# xxxxx_idみたいなやつをオブジェクトにしつつ効率よく取得してくれるやつ
# データ量が多すぎると、メモリは食うかもしれない
sub search {
    my ($class, $where, $cond, ) = @_;
    my $related_row_class = delete $cond->{related_row_class};
    my ($iter, $pager) = $class->_search($where, $cond);

    if ( $related_row_class) {
        my $klass_name_of = {};
        for my $klass_name ( keys %{ $related_row_class } ) {
            my $row_class = $class->base_namespace . "::" . $klass_name;
            $klass_name_of->{$row_class} = [];
        }
        while ( my $row = $iter->next ) {
            for my $klass ( keys %{ $klass_name_of } ) {
                $klass->require
                    or die $@;
                my $col_name = $klass->table_name . "_id";
                push @{ $klass_name_of->{$klass} }, $row->$col_name if $row->can($col_name);
            }
        }
        for my $klass ( keys %{ $klass_name_of } ) {
            if ( !$related_row_class->{$klass} && $klass->can('fetch_multi_by_id') ) {
                $klass_name_of->{$klass} = $klass->fetch_multi_by_id(id => $klass_name_of->{$klass});
            } else {
                my $klass_iter = $klass->search({ 
                        id => $klass_name_of->{$klass} 
                    }, { 
                        no_pager => 1, 
                        related_row_class => $related_row_class->{$klass},
                    });
                $klass_name_of->{$klass} = {};
                while ( my $row = $klass_iter->next ) {
                    $klass_name_of->{$klass}->{$row->id} = $row;
                }
            }
        }
        $iter->reset;
        while ( my $row = $iter->next ) {
            for my $klass ( keys %{ $klass_name_of } ) {
                my $klass_table_name = $klass->table_name;
                my $col = $klass_table_name . "_id";
                # user_idカラムを持つテーブルをオブジェクト化する際に、
                # userメソッドが生えていることが前提です
                if ( $row->can($col) && $row->can($klass_table_name) ) {
                    $row->$klass_table_name($klass_name_of->{$klass}->{$row->$col});
                } else {
                    Carp::croak(sprintf("%s should respond to %s", ref $row, $klass_table_name));
                }
            }
        }
        $iter->reset;
    }

    return wantarray ? ( $iter, $pager ) : $iter;
}

sub count {
    my ($class, $column, $where) = @_;
    $column ||= 'id';
    return $class->get_db(
        {
            write      => 0,
            conditions => $where,
            options    => {},
        }
    )->count($class->table_name, $column, $where);
}

sub single {
    my ($class, $cond, $opt) = @_;
    $opt = {} unless $opt;
    $class->search($cond, +{ %$opt, limit => 1, no_pager => 1 })->first;
}

sub data2itr {
    my ($class, $args) = @_;

    # FIXME: db_masterにすべきか、db_slaveにすべきか
    return $class->get_db(
        {
            write      => 1,
            conditions => {},
            options    => {},
        }
    )->data2itr($class->table_name, $args);
}

# singleのかわりに
sub data2row {
    my ($class, $args) = @_;
    $class->data2itr([$args])->first;
}

sub as_fdat {
    my ($self,) = @_;
    my $hashref = {};
    for my $col ( @{ $self->{select_columns} }) {
        my $value = $self->$col;
        if ( ref $value ) {
            if ( $value->isa('DateTime') ) {
                for my $attr ( qw/ year month day hour minute second / ) {
                    $hashref->{"${col}_$attr"} = $value->$attr;
                }
            }
        } else {
            $hashref->{$col} = $value;
        }
    }

    return $hashref;
}

sub add_trigger {
    my ($class, $name, $code) = @_;
    my $klass = ref $class ? ref $class : $class;

    my $trigger_of = $class->triggers() || {};
    $trigger_of->{$klass} ||= {};
    $trigger_of->{$klass}->{$name} ||= [];

    # 既に同じCoderefが登録されているかどうか調べる
    # ( Apache::Reload対策
    my $already_fg;
    for my $c ( @{ $trigger_of->{$klass}->{$name} } ) {
        if ( $code == $c ) {
            $already_fg++;
        }
    }
    unless ( $already_fg ) {
        push @{ $trigger_of->{$klass}->{$name} }, $code;
        $class->triggers($trigger_of);
    }
}

sub call_trigger {
    my ($class, $name, @args) = @_;
    my $klass = ref $class ? ref $class : $class;
    my $trigger_of = $class->triggers() || {};

    for my $key ( sort keys %{ $trigger_of } ) {
        if ( $klass->isa($key) && $trigger_of->{$key} && $trigger_of->{$key}->{$name} ) {
            for my $code ( @{ $trigger_of->{$key}->{$name} } ) {
                $code->($class, @args);
            }
        }
    }
}

# XXX:
# 基本的に、DBIx::Skinny::Rowからコピペしつつ、$self->{skinny}みてるのをdb_masterをみるように置きかえている
# Skinny側のメソッドが修正されたときのメンテナンスがちょっとなやましいのでメソッドを殺すのもありかなと思ったりしつつ、なんだなんだで便利ではあるのでつけている

sub insert {
    my $self = shift;
    $self->call_trigger('BEFORE_INSERT');
    my $result = $self->get_db(
        {
            write      => 1,
            conditions => $self->get_columns,
            options    => {},
        }
    )->find_or_create($self->{opt_table_info}, $self->get_columns);
    $self->call_trigger('AFTER_INSERT');
    return $result;
}

sub update {
    my ($self, $args, $table) = @_;
    $table ||= $self->{opt_table_info};
    $args ||= $self->get_dirty_columns;
    my $where = $self->_update_or_delete_cond($table);
    my $db = $self->get_db(
        {
            write      => 1,
            conditions => $where,
            options    => {},
        }
    );
    my $txn = $db->txn_scope;

    $self->call_trigger('BEFORE_UPDATE', $args);
    my $result = $db->update($table, $args, $where);
    $self->set($args);
    $self->call_trigger('AFTER_UPDATE', $args);

    $txn->commit;

    return $result;
}

sub delete {
    my ($self, $table) = @_;
    $table ||= $self->{opt_table_info};
    my $where = $self->_update_or_delete_cond($table);
    my $db = $self->get_db(
        {
            write      => 1,
            conditions => $where,
            options    => {},
        }
    );
    my $txn = $db->txn_scope;

    $self->call_trigger('BEFORE_DELETE');
    my $result = $db->delete($table, $where);
    $self->call_trigger('AFTER_DELETE');
    
    $txn->commit;

    return $result;
}

1;

__END__

=head2 SYNOPSIS

    package  YourProj::Skinny;
    use DBIx::Skinny;
    use DBIx::Mixin modules => [qw(Pager SearchWithPager)]; # required.

    package YourProj::Skinny::Row;
    use base qw(DBIx::Skinny::Row::Extended);
    use YourProj::Container;

    sub app_container { YourProj::Container->instance }

