package  DBIx::Skinny::Row::Extended::Plugin::Cache;
use strict;
use warnings;
use Params::Validate qw();
use Carp qw();
use UNIVERSAL::require;

sub import {
    my $pkg = caller;

    {
        no strict 'refs'; ## no critic
        *{"${pkg}::fetch_multi_by_id"} = \&fetch_multi_by_id;
        *{"${pkg}::fetch_multi_by_unique_key"} = \&fetch_multi_by_unique_key;
        *{"${pkg}::cache_key"}    = \&cache_key;
        *{"${pkg}::unique2pk_cache_key"}    = \&unique2pk_cache_key;
        *{"${pkg}::delete_cache"}    = \&delete_cache;
        *{"${pkg}::search_with_cache"}    = \&search_with_cache;
    }
    if ( $pkg->can('add_trigger') ) {
        $pkg->add_trigger(AFTER_UPDATE => sub {
            my ($self, $args) = @_;
            $self->delete_cache;
        });
        $pkg->add_trigger(AFTER_DELETE => sub {
            my ($self, $args) = @_;
            $self->delete_cache;
        });
    }
}

# データを1つ格納するときのキャッシュキー
sub cache_key {
    my ($class, $pk, $table) = @_;
    if ( ref $class ) {
        $class = ref $class;
    }
    $table ||= $class->table_name;
    my $cache_key = join(",", $class, "item", $pk);
    my $expire    = $class->can('cache_expire') ? $class->cache_expire : 60 * 5;
    return wantarray ? ( $cache_key, $expire ) : $cache_key;
}

sub unique2pk_cache_key {
    my ($class, $column_name, $key, $table) = @_;
    if ( ref $class ) {
        $class = ref $class;
    }

    $table ||= $class->table_name;
    my $cache_key = join(",", $class, "unique2pk", $column_name, $key);
    my $expire    = $class->can('cache_expire') ? $class->cache_expire : 60 * 60;
    return wantarray ? ( $cache_key, $expire ) : $cache_key;
}

# 指定したIDのやつを、cacheにあれば、そちらから取得し、
# なければ、DBから取得してくれる
#
# 結果は、IDをkeyにしたHashRefとして返す
sub fetch_multi_by_id {
    my $class = shift;

    my %args = Params::Validate::validate(@_, +{
        id => { type => Params::Validate::ARRAYREF },
    });

    my $cache = $class->app_container->get('cache');
    my $table_name = $class->table_name;
    my (undef, $cache_expire ) = $class->cache_key('');

    my $result_of = {};
    my $cache_key_of = {};
    for my $id ( @{ $args{id} } ) {
        my $key = $class->cache_key($id);
        $cache_key_of->{$key} = $id;
    }
    my $cache_result = $cache->get_multi(keys %{ $cache_key_of });
    for my $cache_key ( keys %{ $cache_result } ) {
        my $id = $cache_key_of->{$cache_key};
        $class->app_container->get('db_master')->profiler->record_query("CACHE GET FOR $cache_key");
        $result_of->{$id} = $class->data2row($cache_result->{$cache_key});
    }

    my @not_cached_item_ids;
    for my $id ( @{ $args{id} } ) {
        if ( ! $result_of->{$id} ) {
            push @not_cached_item_ids, $id;
        } else {
        }
    }
    if ( @not_cached_item_ids ) {
        my $items = $class->search({ id => \@not_cached_item_ids }, { no_pager => 1 });
        while ( my $row = $items->next ) {
            my $key = $class->cache_key($row->id);
            $cache->set($key => $row->get_columns, $cache_expire);
            $class->app_container->get('db_master')->profiler->record_query("CACHE SET FOR $key");
            $result_of->{$row->id} = $row;
        }
    }

    return $result_of;
}

# ユニークキーをidに変換できるcacheがある場合はそれも用いて変換しfetch_multi_by_idでとってくる
# cacheがない場合は、取得しつつ、cacheをセットする
sub fetch_multi_by_unique_key {
    my $class = shift;

    my %args = Params::Validate::validate(@_, +{
        column_name => 1,
        'keys' => +{ type => Params::Validate::ARRAYREF, },
    });

    my $cache_key_of = {};
    my $cache_key_manager = $class->app_container->get('cache_key');
    my $cache = $class->app_container->get('cache');
    for my $key ( @{ $args{keys} } ) {
        my ($cache_key, ) = $class->unique2pk_cache_key($args{column_name}, $key);
        $cache_key_of->{$cache_key} = $key;
    }
    my $cache_result = $cache->get_multi(keys %{ $cache_key_of });

    my @not_cached_keys;
    for my $cache_key ( keys %{ $cache_key_of } ) {
        if ( $cache_result->{$cache_key} ) {
            $class->app_container->get('db_master')->profiler->record_query("CACHE GET FOR $cache_key");
        } else {
            push @not_cached_keys, $cache_key_of->{$cache_key};
        }
    }

    my $data_of = $class->fetch_multi_by_id(id => [ values %{ $cache_result } ]);
    my $result_of = {};
    for my $pk ( keys %{ $data_of } ) {
        my $row = $data_of->{$pk};
        $result_of->{$row->get_column($args{column_name})} = $row;
    }

    if ( @not_cached_keys ) {
        my $not_cache_data = $class->search({
            $args{column_name} => \@not_cached_keys,
        }, { no_pager => 1 });
        while ( my $row = $not_cache_data->next ) {
            my $unique_key = $row->get_column($args{column_name});

            $result_of->{$unique_key} = $row;

            my ($pk_cache_key, $pk_cache_expire) = $row->cache_key($row->id);
            $cache->set($pk_cache_key => $row->get_columns, $pk_cache_expire);
            $class->app_container->get('db_master')->profiler->record_query("CACHE SET FOR $pk_cache_key");

            my ($unique2pk_cache_key, $unique2pk_cache_expire ) = $class->unique2pk_cache_key($args{column_name}, $unique_key);
            $cache->set($unique2pk_cache_key => $row->id, $unique2pk_cache_expire);
            $class->app_container->get('db_master')->profiler->record_query("CACHE SET FOR $unique2pk_cache_key");
        }
    }

    return $result_of;
}

sub delete_cache {
    my ($self, ) = @_;
    my $cache_key_manager = $self->app_container->get('cache_key');
    my $cache = $self->app_container->get('cache');

    $cache->delete($self->cache_key($self->id, $self->table_name));
}

sub search_with_cache {
    my ($class, $where, $cond, ) = @_;
    $cond->{select} = ['id'];
    my ( $iter, $pager ) = $class->search($where, $cond);
    my @ids = [ map { $_->id } $iter->all ];
    my $result_of = $class->fetch_multi_by_id(id => \@ids);
    return (@ids, $result_of, $pager);
}

1;

