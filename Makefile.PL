use 5.006;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME          => 'DBIx::Simple',
    VERSION_FROM  => 'Simple.pm',
    PREREQ_PM     => { 'DBI' => '1.21', 'Attribute::Property' => '1.03' },
    ABSTRACT_FROM => 'Simple.pm',
    AUTHOR        => 'Juerd Waalboer <juerd@cpan.org>'
);
