use strict;

package DBIx::Simple::SQE;
use base 'DBIx::Simple';
use Data::Swap ();
use Carp ();

$Carp::Internal{$_} = 1
    for qw(DBIx::Simple::SQE);

BEGIN { DBIx::Simple->_gimme_regex }  # import $quoted

my $queryfoo;
   $queryfoo       = qr/(?: [^()"']+ | (??{$quoted}) | \( (??{$queryfoo}) \) )*/x;
my $subquery_match = qr/\(\s*(select\s+$queryfoo)\)/i;

sub _interpolate_subqueries {
    my ($self, $query, $binds) = @_;
    while ($$query =~ /$subquery_match/) {
        my $start  = $-[1];
        my $length = $+[1] - $-[1];
        my $pre    = substr($$query, 0, $start);
        my $match  = $1;
        
        substr $$query, $start, $length, join(',',
            map $self->{dbh}->quote($_),
            $self->query(
                $match,
                splice(
                    @$binds,
                    scalar grep($_ eq '?', $pre  =~ /\?|$quoted|[^?'"]+/g),
                    scalar grep($_ eq '?', $match=~ /\?|$quoted|[^?'"]+/g)
                )
            )->flat
        );
        return 0 if not $self->{success};
    }
    return 1;
}

sub query {
    my ($self, $query, @binds) = @_;
    $self->{success} = 0;

    $self->_replace_omniholder(\$query, \@binds);
    $self->_interpolate_subqueries(\$query, \@binds) 
        or return DBIx::Simple::_dummy;

    # The SUPER method will also try to replace the omniholder (there is no
    # other way, because the omniholder stuff HAS to happen first), but in
    # theory there should be no omniholder anymore. Still, it can't hurt to
    # check:

    my $omniholder = $self->omniholder;
    $query =~ /\Q$omniholder/ and Carp::croak(
        'Fatal: omniholder found after interpolating subqueries. Because ' .
        'subquery emulation is deprecated, this is a good moment to think about' .
        'using another database engine. Or to send a patch. Don\'t bother' . 
        'sending a bug report without a patch, though. Sorry.'
    );

    $self->SUPER::query($query, @binds);
}

=head1 NAME

DBIx::Simple::ESQ - Add subquery emulation to DBIx::Simple

=head1 SYNOPSIS

    my $db = DBIx::Simple::ESQ->connect(...);

    my $result = $db->query(
        q[
            SELECT project_name
            FROM   projects
            WHERE  user_id IN (
                SELECT id
                FROM   users
                WHERE  email = ?
            )
            AND    status = ?
        ],
        $email, $status
    );

Is simply a more compact way of doing:

    my $db = DBIx::Simple::ESQ->connect(...);

    my $result = $db->query(
        sprintf(
            q[
                SELECT project_name
                FROM   projects
                WHERE  user_id IN (%s)
                AND    status = ?
            ],
            join(
                ',',
                map(
                    $db->dbh->quote($_),
                    $db->query(
                        q[
                            SELECT id
                            FROM   users
                            WHERE  email = ?
                        ],
                        $email
                    )->flat
                )
            )
        ),
        $status
    );

=head1 DESCRIPTION

B<This module is not actively maintained.> In practice that means that if you
find a bug, you'll have to fix it yourself or live with it. (Patches are
welcome, though). If DBIx::Simple changes in a way that is incompatible with
this module, ESQ will simply stop functioning correctly. Should you want to
take over maintenance of this module, let me know.

This module extends DBIx::Simple by making C<query> emulate nested subqueries
(SELECT only) by executing them and interpolating the results.

This module should not be used if the database provides real subqueries. It is
better to use a database engine that has real subqueries than to use this
module.

Only subqueries like C<(SELECT ...)> (note the parentheses) are interpolated.

Please note that emulation is done by doing multiple queries and is not
atomic, as it would be if the database supported real subqueries. The
queries are executed independently.

=head1 HISTORY

Subquery emulation used to be built into DBIx::Simple itself. It was enabled by
using the C<emulate_subqueries> property (or its alias C<esq>).

Starting from version 1.20, the feature was deprecated. In version 1.23, it was
finally removed. To give users some more time, it was moved to a separate
module.

=head1 LICENSE

There is no license. This software was released into the public domain. Do with
it what you want, but on your own risk. The author disclaims any
responsibility.

=head1 AUTHOR

Juerd Waalboer <juerd@cpan.org> <http://juerd.nl/>

=head1 SEE ALSO

L<DBIx::Simple>

=cut

