package DBIx::Simple;

use 5.006; # qr//, /(??{})/
use DBI;
use Carp;
use strict;
use vars qw($VERSION %results);

my $omniholder = '(??)';

$VERSION = '0.06';

my $quoted = qr/'(?:\\.|[^\\']+)*'|"(?:\\.|[^\\"]+)*"/s;
my $queryfoo = qr/(?: [^()"']+ | (??{$quoted}) | \( (??{$queryfoo}) \) )*/x;
my $subquery_match = qr/\(\s*(select\s+$queryfoo)\)/i;

sub connect {
    my ($class, @arguments) = @_;
    my $self = {
	dbi => DBI->connect(@arguments),
	omniholder => '(??)',
    };
    $self->{dbi}->{TraceLevel}++;
    return undef unless $self->{dbi};
    return bless $self, $class;
}

sub disconnect {
    my ($self) = @_;
    $self->{dbi}->disconnect() if $self->{dbi};
}

sub omniholder {
    my ($self, $value) = @_;
    return $self->{omniholder} = $value if @_ > 1;
    return $self->{omniholder};
}

sub query {
    my ($self, $query, @binds) = @_;
    $self->{success} = 0;
    
    # Replace (??) with (?, ?, ?, ...)
    if (defined $self->{omniholder} and length $self->{omniholder}) {
	my $omniholders = 0;
	my $omniholder = quotemeta $self->{omniholder};
	$query =~ s[($omniholder|$quoted|(?!$omniholder).)]
	{
	    $1 eq $self->{omniholder}
	    ? do {
		croak 'There can be only one omniholder' if $omniholders++;
		'(' . join(', ', ('?') x @binds) . ')';
	    }
	    : $1
	}eg;
    }

    # Subquery emulation
    if ($self->{esq}) {
	while ($query =~ /$subquery_match/) {
	    my $start  = $-[1];
	    my $length = $+[1] - $-[1];
	    my $pre    = substr($query, 0, $start);
	    my $match  = $1;
	    
	    substr $query, $start, $length, join(',',
		map $self->{dbi}->quote($_),
		$self->query(
		    $match,
		    splice(
			@binds,
			scalar grep($_ eq '?', $pre  =~ /\?|$quoted|[^?'"]+/g),
			scalar grep($_ eq '?', $match=~ /\?|$quoted|[^?'"]+/g)
		    )
		)->flat
	    );
	    return DBIx::Simple::Dummy->new() if not $self->{success};
	}
    }
    
    # Actual query
    my $sth = $self->{dbi}->prepare($query) or do {
	$self->{reason} = "Prepare failed ($DBI::errstr)";
	return DBIx::Simple::Dummy->new();
    };
    
    $sth->execute(@binds) or do {
	$self->{reason} = "Execute failed ($DBI::errstr)";
	return DBIx::Simple::Dummy->new();
    };

    $self->{success} = 1;
    my $result;

    # $self is quoted on purpose, to pass along the stringified version,
    # and avoid increasing reference count.
    return $results{$self}{$result} = $result =
	DBIx::Simple::Result->new("$self", $sth);
}

sub begin_work { $_[0]->{dbi}->begin_work() }
sub commit     { $_[0]->{dbi}->commit() }
sub rollback   { $_[0]->{dbi}->rollback() }

sub emulate_subqueries {
    my ($self, $value) = @_;
    $self->{esq} = ! !$value if @_ == 2;
    return $self->{esq};
}
sub esq { goto &emulate_subqueries }

sub DESTROY {
    my ($self) = @_;
    $results{$self}{$_}->DESTROY() for keys %{ $results{$self} };
    $self->disconnect();
}

package DBIx::Simple::Dummy;
use strict;

sub new      { bless \my $dummy, shift }
sub AUTOLOAD { undef }

package DBIx::Simple::Result;
use Carp;
use strict;

sub new {
    my ($class, $db, $sth) = @_;
    # $db should be the stringified object, as a real reference
    # would increase the reference count.
    my $self = {
	db  => $db,
	sth => $sth
    };
    return bless $self, $class;
}

sub list {
    return $_[0]->{sth}->fetchrow_array if wantarray;
    return($_[0]->{sth}->fetchrow_array)[0];
}

sub array {
    return $_[0]->{sth}->fetchrow_arrayref;
}

sub hash {
    return $_[0]->{sth}->fetchrow_hashref;
}

sub flat {
    return map @$_, $_[0]->arrays;
}

sub arrays {
    return @{ $_[0]->{sth}->fetchall_arrayref };
}

sub hashes {
    my ($self) = @_;
    my @return;
    my $dummy;
    push @return, $dummy while $dummy = $self->{sth}->fetchrow_hashref;
    return @return;
}

sub map_hashes {
    my ($self, $keyname) = @_;
    croak 'Key column name not optional' if not defined $keyname;
    my @rows = $self->hashes;
    my @keys;
    for (@rows) {
	push @keys, $_->{$keyname};
	delete $_->{$keyname};
    }
    my %return;
    @return{@keys} = @rows;
    return \%return;
}

sub map_arrays {
    my ($self, $keyindex) = @_;
    $keyindex += 0;
    my @rows = $self->arrays;
    my @keys;
    for (@rows) {
	push @keys, splice @$_, $keyindex, 1;
    }
    my %return;
    @return{@keys} = @rows;
    return \%return;
}

sub map {
    my ($self) = @_;
    return { map { $_->[0] => $_->[1] } $self->arrays };    
}

sub rows {
    my ($self) = @_;
    return $self->{sth}->rows;
}

sub DESTROY {
    my ($self) = @_;
    delete $DBIx::Simple::results{ $self->{db} }{$self} if $self and $self->{db};
    $self->{sth}->finish() if $self->{sth};
    $self->{sth} = undef;
}

1;

=head1 NAME

DBIx::Simple - Easy-to-use OO interface to DBI, capable of emulating subqueries

=head1 SYNOPSIS

=head2 General

    #!/usr/bin/perl -w
    use strict;
    use DBIx::Simple;

    # Instant database with DBD::SQLite
    my $db = DBIx::Simple->connect('dbi:SQLite:dbdbname=file.dat');

    # MySQL database
    my $db = DBIx::Simple->connect(
	'DBI:mysql:database=test',     # DBI source specification
	'test', 'test',                # Username and password
	{ RaiseError => 1 }            # Additional options
    );

    # Abstracted example: $db->query($query, @variables)->what_you_want;

=head2 Simple Queries

    $db->query('DELETE FROM foo WHERE id = ?', $id);
    die $db->{reason} if not $db->{success};

    for (1..100) {
	$db->query(
	    'INSERT INTO randomvalues VALUES (?, ?)',
	    int rand(10),
	    int rand(10)
	);
    }

    $db->query(
	'INSERT INTO sometable VALUES (??)',
	$first, $second, $third, $fourth, $fifth, $sixth
    );
    # (??) is expanded to (?, ?, ?, ?, ?, ?) automatically

=head2 Single row queries

    my ($two)          = $db->query('SELECT 1 + 1')->list;
    my ($three, $four) = $db->query('SELECT 3, 2 + 2')->list;

    my ($name, $email) = $db=>query(
	'SELECT name, email FROM people WHERE email = ? LIMIT 1',
	$mail
    )->list;

=head2 Fetching all rows in one go

=head3 One big flattened list (primarily for single column queries)

    my @names = $db->query('SELECT name FROM people WHERE id > 5')->flat;

=head3 Rows as array references

    for my $row ($db->query('SELECT name, email FROM people')->arrays) {
	print "Name: $row->[0], Email: $row->[1]\n";
    }

=head3 Rows as hash references

    for my $row ($db->query('SELECT name, email FROM people')->hashes) {
	print "Name: $row->{name}, Email: $row->{email}\n";
    }

=head2 Fetching one row at a time

=head3 Rows as lists

    {
	my $result = $db->query('SELECT name, email FROM people');
	while (my @row = $result->list) {
	    print "Name: $row[0], Email: $row[1]\n";
	}
    }

=head3 Rows as array references

    {
	my $result = $db->query('SELECT name, email FROM people');
	while (my $row = $result->array) {
	    print "Name: $row->[0], Email: $row->[1]\n";
	}
    }

=head3 Rows as hash references

    {
	my $result = $db->query('SELECT name, email FROM people');
	while (my $row = $result->hash) {
	    print "Name: $row->{name}, Email: $row->{email}\n";
	}
    }

=head2 Building maps (also fetching all rows in one go)

=head3 A hash of hashes

    my $customers =
	$db
	-> query('SELECT id, name, location FROM people')
	-> map_hashes('id');

    # $customers = { $id => { name => $name, location => $location } }

=head3 A hash of arrays

    my $customers =
	$db
	-> query('SELECT id, name, location FROM people')
	-> map_arrays(0);

    # $customers = { $id => [ $name, $location ] }

=head3 A hash of values (two-column queries)

    my $names =
	$db
	-> query('SELECT id, name FROM people')
	-> map;

    # $names = { $id => $name }

=head2 Subquery emulation

    $db->esq(1);
    my @projects = $db->query(
	'
	    SELECT project_name
	    FROM   projects
	    WHERE  user_id = (
		SELECT id
		FROM   users
		WHERE  email = ?
	    )
	',
	$email
    )->flat;

=head1 DESCRIPTION

This module is aimed at ease of use, not at SQL abstraction or
efficiency. The only thing this module does is provide a bone easy
interface to the already existing DBI module. With DBIx::Simple, the
terms dbh and sth are not used in the documentation (except for this
description), although they're omnipresent in the module's source.  You
don't have to think about them.

A query returns a result object, that can be used directly to pick the
sort of output you want.  There's no need to check if the query
succeeded in between calls, you can stack them safely, and check for
success later. This is because failed queries have dummy results,
objects of which all methods return undef.

=head2 DBIx::Simple object methods

=over 10

=item C<< DBIx::Simple->connect( ... ) >>

This argument takes the exact arguments a normal C<< DBI->connect >>
would take. It's the constructor method, and it returns a new
DBIx::Simple object.  See also L<DBI>.

=item C<query($query, @values)>

This calls DBI's C<prepare> and C<execute> methods, passing the values
along to replace C<?> placeholders.  C<query> returns a new
DBIx::Simple::Result object (or DBIx::Simple::Dummy), that can be used
immediately to get data out of it.  You should always use placeholders
instead of the variables themselves, as DBI will automatically quote and
escape the values.

DBIx::Simple provides an omniholder placeholder that will expand to
C<(?, ?, ...)> with as many question marks as @values. There can be only
one omniholder, and since it uses all given values, you shouldn't
combine it with normal placeholders. This feature was inspired by the
EZDBI module.

=item C<omniholder($new_value)>

This sets the omniholder string. Use C<undef> or an empty string to
disable this feature. Please note that the given $new_value is not a
regex. The default omniholder is C<(??)>.

=item C<emulate_subqueries($bool)>, C<esq($bool)> - EXPERIMENTAL

Sets if DBIx::Simple should enable subquery emulation. Many databases,
like Postgres and SQLite have support for subqueries built in. Some,
like MySQL, have not. True (C<1>) enables, false (C<0>) disables.
Defaults to false.

In normal MySQL, one would probably use C<SELECT projects.project FROM
projects, users WHERE project.user_id = users.id AND user.email = ?> to
select the projects that belong to the user with a certain email
address. Postgres people would write C<SELECT project FROM projects
WHERE user_id = (SELECT id FROM users WHERE email = ?)> instead.

Subqueries can make complex queries readable, but MySQL doesn't have
them and many people use MySQL. Now they can have subqueries too!

Emulation is done by simply doing multiple queries.

B<< This feature is experimental. Please let me know if it works and if
you like it. Send your comments to <juerd@cpan.org>. Even if everything
goes well, I'd like to get some feedback. >>

=item C<commit>, C<rollback>

These just call the DBI methods and Do What You Mean.

=item C<disconnect>

Does What You Mean. Also note that the connection is automatically
terminated when the object is destroyed (C<undef $db> to do so
explicitly), and that all statements are also finished when the object
is destroyed. C<disconnect> Does not destroy the object.

=back

=head2 DBIx::Simple::Result object methods

=over 10

=item C<new>

The constructor should only be called internally, by DBIx::Simple
itself. Some simple minded garbage collection is done in DBIx::Simple,
and you shouldn't be directly creating your own result objects. The
curious are encouraged to read the module's source code to find out what
the arguments to C<new> are.

=item C<list>

C<list> Returns a list of elements in a single row. This is like a
dereferenced C<< $result->array >>. In scalar context, returns only the
first value of the row.

=item C<array> and C<hash>

These methods return a single row, in an array reference, or a hash
reference, respectively.  Internally, C<fetchrow_arrayref> or
C<fetchrow_hashref> is used.

=item C<flat>

C<flat> Returns a list of all returned fields, flattened. This can be
very useful if you select a single column. Consider C<flat> to be
C<list>'s plural.

=item C<arrays> and C<hashes>

These methods return a list of rows of array or hash references.
Internally, C<fetchall_arrayref> is dereferenced, or a lot of
C<fetchrow_hashref> returns are accumulated.

=item C<map_arrays($column_number)> and C<map_hashes($column_name)>

These methods build a hash, with the chosen column as keys, and the
remaining columns in array or hash references as values. For
C<map_arrays>, the column number is optional and defaults to 0 (the
first column). The methods return a reference to the built hash.

=item C<map>

Returns a reference to a hash that was built using the first two columns
as key/value pairs. Use this only if your query returns two values per
row (other values will be discarded).

=item C<rows>

Returns the number of rows. This function calls DBI's rows method, and
may not do what you want. See L<DBI> for a good explanation.

=item finish?

There is no finish method. To finish the statement, just let the object
go out of scope (you should always use C<my>, and C<use strict>) or
destroy it explicitly using C<undef $result>.

=back

=head1 FEEDBACK

I'd like to hear from you what you think about DBIx::Simple, and if it
has made your life easier :). If you find serious bugs, let me know.  If
you think an important feature is missing, let me know (but I'm not
going to implement functions that aren't used a lot, or that are only
for effeciency, because this module has only one goal: simplicity). My
email address can be found near the end of this document.

=head1 BUGS

Nothing is perfect, but let's try to create perfect things. Of course,
this module shares all DBI bugs. If you want to report a bug, please try
to find out if it's DBIx::Simple's fault or DBI's fault first, and don't
report DBI bugs to me.

Note: the map functions do not check if the key values are unique. If
they are not, keys are overwritten.

=head1 DISCLAIMER

No warranty, no guarantees. I hereby disclaim all responsibility for
what might go wrong.

=head1 AUTHOR

Juerd <juerd@cpan.org>

=head1 SEE ALSO

L<DBI>

=cut

