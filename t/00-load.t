use v6;
use Test;
use Sparky::JobApi;

plan 1;

my $j = Sparky::JobApi.new(:workers<10>);

isa-ok($j, Sparky::JobApi);

say $j.info.perl;

done-testing;


