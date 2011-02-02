package DBIx::Skinny::Row::Extended::Plugin::MogileFS;
use strict;
use warnings;

sub import {
    my $pkg = caller;

    # TODO: import時に必要なcontainerがチェックしてくれる仕組みがあるとよいのかなぁ...
    {
        no strict 'refs'; ## no critic
        *{"${pkg}::mogile_key"}            = \&mogile_key;
        *{"${pkg}::mogile_list_keys"}      = \&mogile_list_keys;
        *{"${pkg}::upload"}                = \&upload;
        *{"${pkg}::delete_mogile"}         = \&delete_mogile;
        *{"${pkg}::delete_mogile_related"} = \&delete_mogile_related;
    }

    if ( $pkg->can('add_trigger') ) {
        $pkg->add_trigger('BEFORE_DELETE', sub {
            my ($self, ) = @_;
            $self->delete_mogile_related;
        });
    }
}

sub mogile_key {
    my $self = shift;

    my $table_name = $self->table_name;
    sprintf("%s;%s", $table_name, $self->id);
}

sub mogile_list_keys {
    my $self = shift;
    return $self->mogile->list_keys($self->mogile_key);
}

# データを登録しつつ、MogileFSにデータ保存する
# このテーブルにinsertする際は、常にこのメソッドを使うべし
sub upload {
    my ($class, $path, $args) = @_;

    my $photo;
    my $db = $class->get_db(
        {
            write      => 1,
            conditions => $args,
            options    => {},
        }
    );
    my $txn = $db->txn_scope;
    {
        $photo = $db->insert($class->table_name => $args);

        my $key = $photo->mogile_key;
        if ( ref $path && ref $path eq "SCALAR" ) {
            $class->mogile->store_content($key, 'normal', $path)
                or die $class->mogile->errstr;

        } else {
            $class->mogile->store_file($key, 'normal', $path)
                or die $class->mogile->errstr;

        }
        $photo->call_trigger('AFTER_UPLOAD', $path);
    }
    $txn->commit;

    return $photo;
}

sub delete_mogile {
    my ($self, ) = @_;

    $self->mogile->delete($self->mogile_key)
        or die $self->mogile->errstr;

}

# 関連するキーも含めて全て画像を消す
sub delete_mogile_related {
    my $self = shift;

    my $keys = $self->mogile_list_keys;
    for my $key ( @{ $keys } ) {
        $self->mogile->delete($key)
            or die $self->mogile->errstr;

    }
}

1;

__END__

=head2 DESCRIPTION

YourProj::Model::DB::Row::xxxxにMogileFSの操作に関連するメソッドを生やします

=head2 SYNOPSIS

    package YourProj::Model::DB::Row;
    use YourProj::Model::DB::Row::Plugin::MogileFS;

    package main;

    my $photo = YourProj::Model::DB::Row::Photo->upload($imgpath, { foo => 'bar' });
    
    my $key = $photo->mogile_key;
    @paths = container('mogile')->get_paths($key);
    my $thumb_key = $photo->mogile_key(thumbnail => 1);

