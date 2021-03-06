use inc::Module::Install;
use Module::Install::ReadmeFromPod;
use Module::Install::Repository;
name 'DBIx-Skinny-Row-Extended';
all_from 'lib/DBIx/Skinny/Row/Extended.pm';
readme_from 'lib/DBIx/Skinny/Row/Extended.pm';

# requires '';
requires 'DBIx::Skinny';
requires 'DBIx::Skinny::Mixin::Pager';

tests_recursive 't';
author_tests 'xt';

build_requires 'Test::More';
build_requires 'Test::LoadAllModules';
build_requires 'Module::Install::ReadmeFromPod';
use_test_base;
auto_include;
auto_set_repository;
WriteAll;
