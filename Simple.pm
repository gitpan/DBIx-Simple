use 5.006;
use strict;
use DBI;


package DBIx::Simple;
use Carp;

our $VERSION = '0.10';

my $quoted = qr/'(?:\\.|[^\\']+)*'|"(?:\\.|[^\\"]+)*"/s;
my $queryfoo = qr/(?: [^()"']+ | (??{$quoted}) | \( (??{$queryfoo}) \) )*/x;
my $subquery_match = qr/\(\s*(select\s+$queryfoo)\)/i;

my %statements;

sub EDEAD {
    sprintf "Database object no longer usable (because of %s)",
            $_[0]->{cause_of_death} || 'UNKNOWN'
}

sub connect {
    my ($class, @arguments) = @_;
    my $self = { omniholder => '(??)' };
    if (defined $arguments[0] and UNIVERSAL::isa($arguments[0], 'DBI::db')) {
	$self->{dbi} = shift @arguments;
	carp "Additional arguments for $class->connect are ignored"
	    if @arguments;
    } else {
	$arguments[3]->{PrintError} = 0
	    unless defined $arguments[3] and defined $arguments[3]{PrintError};
	$self->{dbi} = DBI->connect(@arguments);
    }
    return undef unless $self->{dbi};
    return bless $self, $class;
}

sub new { goto &connect; }

sub omniholder {
    my ($self, $value) = @_;
    return $self->{omniholder} = $value if @_ > 1;
    return $self->{omniholder};
}

sub emulate_subqueries {
    my ($self, $value) = @_;
    $self->{esq} = ! !$value if @_ == 2;
    return $self->{esq};
}

sub esq { goto &emulate_subqueries }

sub query {
    my ($self, $query, @binds) = @_;
    croak $self->EDEAD if $self->{dead};
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

    # Subquery interpolation
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
    my $sth = eval { $self->{dbi}->prepare($query) } or do {
	if ($@) {
	    $@ =~ s/ at \S+ line \d+\.\n\z//;
	    croak $@;
	}
	$self->{reason} = "Prepare failed ($DBI::errstr)";
	return DBIx::Simple::Dummy->new();
    };
    
    eval { $sth->execute(@binds) } or do {
	if ($@) {
	    $@ =~ s/ at \S+ line \d+\.\n\z//;
	    croak $@;
	}
	$self->{reason} = "Execute failed ($DBI::errstr)";
	return DBIx::Simple::Dummy->new();
    };

    $self->{success} = 1;

    # $self is quoted on purpose, to pass along the stringified version,
    # and avoid increasing reference count.
    my $st = DBIx::Simple::Statement->new("$self", $sth);
    $statements{$self}{$st} = $st;
    return DBIx::Simple::Result->new($st);
}

sub error {
    croak $_[0]->EDEAD if $_[0]->{dead};
    my $error = 'DBI error: ' . $_[0]->{dbi}->errstr;
    return $error;
}

sub begin_work { croak $_[0]->EDEAD if $_[0]->{dead}; $_[0]->{dbi}->begin_work }
sub commit     { croak $_[0]->EDEAD if $_[0]->{dead}; $_[0]->{dbi}->commit     }
sub rollback   { croak $_[0]->EDEAD if $_[0]->{dead}; $_[0]->{dbi}->rollback   }
sub func       { croak $_[0]->EDEAD if $_[0]->{dead};
                                                $_[0]->{dbi}->func(@_[1..$#_]) }

sub die {
    my ($self, $cause) = @_;
    return if $self->{dead};
    $statements{$self}{$_}->die($cause) for keys %{ $statements{$self} };
    delete $statements{$self};
    $self->{dbi}->disconnect() if defined $self->{dbi};  # XXX
    $self->{dead} = 1;
    $self->{cause_of_death} = $cause;
}

sub disconnect {
    my ($self) = @_;
    croak $self->EDEAD if $self->{dead};
    $self->die(sprintf "$self->disconnect at %s line %d", (caller)[1, 2]);
}

sub DESTROY {
    my ($self) = @_;
    $self->die(sprintf "$self->DESTROY at %s line %d", (caller)[1, 2]);
}


package DBIx::Simple::Dummy;

use overload
    '""' => sub { shift },
    bool => sub { 0 };

sub new      { bless \my $dummy, shift }
sub AUTOLOAD { return }


package DBIx::Simple::Statement;

sub new {
    my ($class, $db, $sth) = @_;
    my $self = {
	sth  => $sth,
	db   => $db,
	dead => 0,
    };
    return bless $self, $class;
}

sub die {
    my ($self, $reason) = @_;
    return if $self->{dead};
    $self->{sth}->finish();
    delete $statements{ $self->{db} }{ $self };
    $self->{dead} = 1;
    $self->{cause_of_death} = $reason;
}

sub DESTROY {
    goto &die;
}    


package DBIx::Simple::Result;
use Carp;

sub EDEAD {
    sprintf "Result object no longer usable (because of %s)",
            $_[0]->{st}->{cause_of_death} || 'UNKNOWN'
}

sub new {
    my ($class, $st) = @_;
    # $db should be the stringified object, as a real reference
    # would increase the reference count.
    my $self = {
	st  => $st,
    };
    return bless $self, $class;
}

sub list {
    croak $_[0]->EDEAD if $_[0]->{st}->{dead};
    return $_[0]->{st}->{sth}->fetchrow_array if wantarray;
    return($_[0]->{st}->{sth}->fetchrow_array)[0];
}

sub array {
    croak $_[0]->EDEAD if $_[0]->{st}->{dead};
    return $_[0]->{st}->{sth}->fetchrow_arrayref;
}

sub hash {
    croak $_[0]->EDEAD if $_[0]->{st}->{dead};
    return $_[0]->{st}->{sth}->fetchrow_hashref;
}

sub flat {
    croak $_[0]->EDEAD if $_[0]->{st}->{dead};
    return map @$_, $_[0]->arrays;
}

sub arrays {
    croak $_[0]->EDEAD if $_[0]->{st}->{dead};
    return @{ $_[0]->{st}->{sth}->fetchall_arrayref };
}

sub hashes {
    my ($self) = @_;
    croak $self->EDEAD if $self->{st}->{dead};
    my @return;
    my $dummy;
    push @return, $dummy while $dummy = $self->{st}->{sth}->fetchrow_hashref;
    return @return;
}

sub map_hashes {
    my ($self, $keyname) = @_;
    croak $self->EDEAD if $self->{st}->{dead};
    croak 'Key column name not optional' if not defined $keyname;
    my @rows = $self->hashes;
    my @keys;
    for (@rows) {
	push @keys, $_->{$keyname};
	delete $_->{$keyname};
    }
    my %return;
    @return{@keys} = @rows;
    return wantarray ? %return : \%return;
}

sub map_arrays {
    my ($self, $keyindex) = @_;
    croak $self->EDEAD if $self->{st}->{dead};
    $keyindex += 0;
    my @rows = $self->arrays;
    my @keys;
    for (@rows) {
	push @keys, splice @$_, $keyindex, 1;
    }
    my %return;
    @return{@keys} = @rows;
    return wantarray ? %return : \%return;
}

sub map {
    my ($self) = @_;
    croak $self->EDEAD if $self->{st}->{dead};
    my %return = map { $_->[0] => $_->[1] } $self->arrays;
    return wantarray ? %return : \%return;
}

sub rows {
    my ($self) = @_;
    croak $self->EDEAD if $self->{st}->{dead};
    return $self->{st}->{sth}->rows;
}

sub finish {
    my ($self) = @_;
    croak $self->EDEAD if $self->{st}->{dead};
    $self->{st}->die(sprintf "$self->finish at %s line %d", (caller)[1, 2]);
}

sub DESTROY {
    my ($self) = @_;
    return if $self->{st}->{dead};
    $self->{st}->die(sprintf "$self->DESTROY at %s line %d", (caller)[1, 2]);
}

1;

=head1 NAME

DBIx::Simple - Easy-to-use OO interface to DBI, capable of emulating subqueries

=head1 SYNOPSIS

=head2 OVERVIEW

=head3 DBIx::Simple
 
    $db = DBIx::Simple->connect(...)  # or ->new

    $db->omniholder         $db->emulate_subqueries  # or ->esq 

    $db->begin_work         $db->commit
    $db->rollback           $db->disconnect
    $db->func(...)

    $result = $db->query(...)

=head3 DBIx::Simple::Result

    @row = $result->list    @rows = $result->flat
    $row = $result->array   @rows = $result->arrays
    $row = $result->hash    @rows = $result->hashes

    %map = $result->map_arrays(...)
    %map = $result->map_hashes(...)
    %map = $result->map

    $rows = $result->rows

    $result->finish

=head2 EXAMPLES

=head3 General

    #!/usr/bin/perl -w
    use strict;
    use DBIx::Simple;

    # Instant database with DBD::SQLite
    my $db = DBIx::Simple->connect('dbi:SQLite:dbname=file.dat');

    # Connecting to a MySQL database
    my $db = DBIx::Simple->connect(
	'DBI:mysql:database=test',     # DBI source specification
	'test', 'test',                # Username and password
	{ RaiseError => 1 }            # Additional options
    );

    # Using an existing database handle
    my $db = DBIx::Simple->connect($dbh);

    # Abstracted example: $db->query($query, @variables)->what_you_want;

    $db->commit or die $db->error;

=head3 Simple Queries

    $db->query('DELETE FROM foo WHERE id = ?', $id) or die $db->error;

    for (1..100) {
	$db->query(
	    'INSERT INTO randomvalues VALUES (?, ?)',
	    int rand(10),
	    int rand(10)
	) or die $db->error;
    }

    $db->query(
	'INSERT INTO sometable VALUES (??)',
	$first, $second, $third, $fourth, $fifth, $sixth
    );
    # (??) is expanded to (?, ?, ?, ?, ?, ?) automatically

=head3 Single row queries

    my ($two)          = $db->query('SELECT 1 + 1')->list;
    my ($three, $four) = $db->query('SELECT 3, 2 + 2')->list;

    my ($name, $email) = $db=>query(
	'SELECT name, email FROM people WHERE email = ? LIMIT 1',
	$mail
    )->list;

=head3 Fetching all rows in one go

=head4 One big flattened list (primarily for single column queries)

    my @names = $db->query('SELECT name FROM people WHERE id > 5')->flat;

=head4 Rows as array references

    for my $row ($db->query('SELECT name, email FROM people')->arrays) {
	print "Name: $row->[0], Email: $row->[1]\n";
    }

=head4 Rows as hash references

    for my $row ($db->query('SELECT name, email FROM people')->hashes) {
	print "Name: $row->{name}, Email: $row->{email}\n";
    }

=head3 Fetching one row at a time

=head4 Rows as lists

    {
	my $result = $db->query('SELECT name, email FROM people');
	while (my @row = $result->list) {
	    print "Name: $row[0], Email: $row[1]\n";
	}
    }

=head4 Rows as array references

    {
	my $result = $db->query('SELECT name, email FROM people');
	while (my $row = $result->array) {
	    print "Name: $row->[0], Email: $row->[1]\n";
	}
    }

=head4 Rows as hash references

    {
	my $result = $db->query('SELECT name, email FROM people');
	while (my $row = $result->hash) {
	    print "Name: $row->{name}, Email: $row->{email}\n";
	}
    }

=head3 Building maps (also fetching all rows in one go)

=head4 A hash of hashes

    my $customers =
	$db
	-> query('SELECT id, name, location FROM people')
	-> map_hashes('id');

    # $customers = { $id => { name => $name, location => $location } }

=head4 A hash of arrays

    my $customers =
	$db
	-> query('SELECT id, name, location FROM people')
	-> map_arrays(0);

    # $customers = { $id => [ $name, $location ] }

=head4 A hash of values (two-column queries)

    my $names =
	$db
	-> query('SELECT id, name FROM people')
	-> map;

    # $names = { $id => $name }

=head3 Subquery emulation

    $db->esq(1);
    my @projects = $db->query(q{
	SELECT project_name
	FROM   projects
	WHERE  user_id = (
	    SELECT id
	    FROM   users
	    WHERE  email = ?
	)
    }, $email )->flat;

=head1 DESCRIPTION

DBIx::Simple provides a simplified interface to DBI, Perl's powerful database
module.

This module is aimed at rapid development and easy maintenance. Query
preparation and execution are combined in a single method, the result object
(which is a wrapper around the statement handle) provides easy row-by-row and
slurping methods. 

The C<query> method returns either a result object, or a dummy object. The
dummy object returns undef (or an empty list) for all methods and when used in
boolean context, is false. The dummy object lets you postpone (or skip) error
checking, but it also makes immediate error checking a simple C<< 
$db->query(...) or die $db->{reason} >>.

For users of poorly equipped databases (like MySQL), DBIx::Simple provides
emulation of subqueries by interpolating intermediate results. For users of
better database systems (like SQLite and PostgreSQL), the module provides
direct access to DBI's transaction related methods.

=head2 DBIx::Simple methods

=over 10

=item C<< DBIx::Simple->connect($dbh) >>

=item C<< DBIx::Simple->connect($dsn, $user, $pass, \%options) >>

The C<connect> or C<new> class method takes either an existing DBI object
($dbh), or a list of arguments to pass to C<< DBI->connect >>. See L<DBI> for a
detailed description.

You cannot use this method to clone a DBIx::Simple object: the $dbh passed
should be a DBI::db object, not a DBIx::Simple object.

This method is the constructor and returns a DBIx::Simple object on success. On
failure, it returns undef.

=item C<omniholder($new_value)>

This returns the omniholder string, after setting a new string if one is given.
Use a $new_value of C<undef> or an empty string to disable the omniholder
feature. Note that the given $new_value is not a regular expression. The
default omniholder is C<(??)>.

As shown in the SYNOPSIS, you can use an omniholder to avoid having to count
question marks. In a query, C<(??)> (or whatever string you set using this
method) is replaced with C<(?, ?, ?, ...)>, with as many question marks as
@values passed to the query method (see below).

=item C<emulate_subqueries($bool)>, C<esq($bool)>

DBIx::Simple can emulate nested subqueries (SELECT only) by executing them and
interpolating the results. This methods enables or disables this feature.
Subquery emulation is disabled by default, and should not be used if the
database provides real subqueries.

Only subqueries like C<(SELECT ...)> (note the parentheses) are interpolated.

Please note that emulation is done by doing multiple queries and is not
atomic, as it would be if the database supported real subqueries. The
queries are executed independently.

=item C<error>

Returns the error string of the last DBI method. See the discussion of "C<err>"
and "C<errstr>" in L<DBI>.

=item C<query($query, @values)>

The C<query> method pepares and executes the query and returns a result object.

If an omniholder (see above) is present in the query, it is replaced with a
list of as many question marks as @values. If subquery emulation (see above) is
enabled, subquery results are interpolated in the main query before the main
query is executed.

The database drivers substitute placeholders (question marks that do not appear
in quoted literals) in the query with the given @values, after them escaping
them. You should always use placeholders, and never use user input in database
queries.

On success, returns a DBIx::Simple::Result object and sets $db->{success} to 1.

On failure, returns a DBIx::Simple::Dummy object and sets $db->{success} to 0.

=item C<begin_work>, C<commit>, C<rollback>

These transaction related methods call the DBI respective methods and
Do What You Mean. See L<DBI> for details.

=item C<func(...)>

This calls the C<func> method of DBI. See L<DBI> for details.

=item C<disconnect>

Destroys (finishes) active statements and disconnects. Whenever the database
object is destroyed, this happens automatically. After disconnecting, you can
no longer use the database object or any of its result object.

=back

=head2 DBIx::Simple::Dummy

The C<query> method of DBIx::Simple returns a dummy object on failure. Its
methods all return an empty list or undef, depending on context. When used in
boolean context, a dummy object evaluates to false.

=head2 DBIx::Simple::Result methods

=over 10

=item C<list>

Fetches a single row and returns a list of values. In scalar context, this
returns only the first value.

=item C<array>

Fetches a single row and returns an array reference.

=item C<hash>

Fetches a single row and returns a hash reference.

=item C<flat>

Fetches all remaining rows and returns a flattened list.

=item C<arrays>

Fetches all remaining rows and returns a list of array references.

=item C<hashes>

Fetches all remaining rows and returns a list of hash references. 

=item C<map_arrays($column_number)>

Constructs a hash of array references keyed by the values in the chosen column.

In scalar context, returns a reference to the hash.

=item C<map_hashes($column_name)>

Constructs a hash of hash references keyed by the values in the chosen column.

In scalar context, returns a reference to the hash.

=item C<map>

Constructs a simple hash, using the first two columns as key/value pairs.
Should only be used with queries that return two columns.

In scalar context, returns a reference to the hash.

=item C<rows>

Returns the number of rows affected by the last row affecting command, or -1 if
the number of rows is not known or not available.

For SELECT statements, it is generally not possible to know how many rows are
returned. MySQL does provide this information. See L<DBI> for a detailed
explanation.

=item C<finish>

Finishes the statement. After finishing a statement, it can no longer be used.
When the result object is destroyed, its statement handle is automatically
finished and destroyed. There should be no reason to call this method
explicitly; just let the result object go out of scope.

=back

=head1 BUGS / TODO

Although this module has been tested thoroughly in production environments, it
still has no automated test suite. If you want to write tests, please contact
me.

The mapping methods do not check whether the keys are unique. Rows that are
fetched later overwrite earlier ones.

PrintError is disabled by default. If you enable it, beware that it will report
line numbers in DBIx/Simple.pm. 

Note: this module does not provide any SQL abstraction and never will. If you
don't want to write SQL queries, use DBIx::Abstract.

=head1 DISCLAIMER

I disclaim all responsibility. Use this module at your own risk.

=head1 AUTHOR

Juerd <juerd@cpan.org> - <http://juerd.nl/>

Do you like DBIx::Simple? Or hate it? Have you found a bug? Do you want to
suggest a feature? I love receiving e-mail from users, so please drop me a
line. 

=head1 SEE ALSO

L<perl>, L<perlref>, L<DBI>

=cut

