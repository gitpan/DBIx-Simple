#-----------------------#
  package DBIx::Simple;
#-----------------------#
use DBI;
use strict;

our $VERSION = '0.02';

our %results;

sub connect {
    my ($class, @arguments) = @_;
    my $self = {
	dbi => DBI->connect(@arguments)
    };
    return undef unless $self->{dbi};
    return bless $self, $class;
}

sub disconnect {
    my ($self) = @_;
    $self->{dbi}->disconnect() if $self->{dbi};
}

sub query {
    my ($self, $query, @binds) = @_;
    $self->{success} = 0;
    
    my $sth = $self->{dbi}->prepare($query) or do {
	$self->{reason} = 'Prepare failed';
	return DBIx::Simple::Dummy->new();
    };
    
    $sth->execute(@binds) or do {
	$self->{reason} = 'Execute failed';
	return DBIx::Simple::Dummy->new();
    };

    $self->{success} = 1;
    my $result;

    # $self is quoted on purpose, to pass along the stringified version,
    # and avoid increasing reference count.
    return $results{$self}{$result} = $result = DBIx::Simple::Result->new("$self", $sth);
}

sub commit {
    my ($self) = @_;
    $self->{dbi}->commit();
}

sub rollback {
    my ($self) = @_;
    $self->{dbi}->rollback();
}

sub DESTROY {
    my ($self) = @_;
    $results{$self}{$_}->DESTROY() for keys %{ $results{$self} };
    $self->disconnect();
}

#------------------------------#
  package DBIx::Simple::Dummy;
#------------------------------#
use strict;

sub new      { bless \my $dummy, shift }
sub AUTOLOAD { undef }

package DBIx::Simple::Result;
use Carp;
use strict;

sub new {
    my ($class, $db, $sth) = @_;
    my $self = {
	db  => $db,
	sth => $sth
    };
    return bless $self, $class;
}

sub list {
    my ($self) = @_;
    return $self->{sth}->fetchrow_array;
}

sub array {
    my ($self) = @_;
    return $self->{sth}->fetchrow_arrayref;
}

sub hash {
    my ($self) = @_;
    return $self->{sth}->fetchrow_hashref;
}

sub flat {
    my ($self) = @_;
    return map @$_, $self->arrays;
}

sub arrays {
    my ($self) = @_;
    return @{ $self->{sth}->fetchall_arrayref };
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

'Spirit moves through all things';

__END__

=head1 NAME

DBIx::Simple - An easy-to-use, object oriented interface to DBI

=head1 SYNOPSIS

    #!/usr/bin/perl -w
    use strict;
    use DBIx::Simple;

    my $db = DBIx::Simple->connect(
	'DBI:mysql:database=test',     # DBI source specification
	'test', 'test',                # Username and password
	{ PrintError => 1 }            # Additional options
    );

    #### SIMPLE QUERIES

    $db->query('DELETE FROM foo');
    die "$db->{reason} ($DBI::errstr)" if not $db->{success};

    for (1..100) {
	$db->query(
	    'INSERT INTO foo VALUES (?, ?)',
	    int rand(10),
	    int rand(10)
	);
    }

    #### SINGLE ROW QUERIES

    my ($two)          = $db->query('SELECT 1 + 1')->list;
    my ($three, $four) = $db->query('SELECT 3, 2 + 2')->list;

    #### FETCHING ALL IN ONE GO

    my @names = $db->query('SELECT name FROM people WHERE foo > 5')->flat;

    for my $row ($db->query('SELECT field1, field2 FROM foo')->arrays) {
	print "--> $row->[0], $row->[1]\n";
    }

    for my $row ($db->query('SELECT field1, field2 FROM foo')->hashes) {
	print "--> $row->{field1}, $row->{field2}\n";
    }

    #### FETCHING ONE ROW AT A TIME

    {
	my $result = $db->query('SELECT field1, field2 FROM foo');
	while (my $row = $result->array) {
	    print "--> $row->[0], $row->[1]\n";
	}
    }

    {
	my $result = $db->query('SELECT field1, field2 FROM foo');
	while (my $row = $result->hash) {
	    print "--> $row->{field1}, $row->{field2}\n";
	}
    }

    #### BUILDING MAPS (also fetching all in one go)

    # Hash of hashes
    my $customers =
	$db
	-> query('SELECT id, name, location FROM people')
	-> map_hashes('id');

    # Hash of arrays
    my $customers =
	$db
	-> query('SELECT id, name, location FROM people')
	-> map_arrays(0);

    # Hash of values
    my $names =
	$db
	-> query('SELECT id, name FROM people')
	-> map;

=head1 DESCRIPTION

This module is aimed at ease of use, not at SQL abstraction or
efficiency. The only thing this module does is provide a bone easy
interface to the already existing DBI module. With DBIx::Simple, the
terms dbh and sth are not used in the documentation (except for this
description), although they're omnipresent in the module's source.
You don't have to think about them.

A query returns a result object, that can be used directly to pick
the sort of output you want.  There's no need to check if the query
succeeded in between calls, you can stack them safely, and check for
success later. This is because failed queries have dummy results,
objects of which all methods return undef.

=head2 DBIx::Simple object methods

=over 10

=item C<DBIx::Simple-E<gt>connect( ... )>

This argument takes the exact arguments a normal DBI->connect()
would take. It's the constructor method, and it returns a new
DBIx::Simple object.

=item C<query($query, @values)>

This calls DBI's prepare() and execute() methods, passing the values
along to replace placeholders.  query() returns a new
DBIx::Simple::Result object (or DBIx::Simple::Dummy), that can be
used immediately to get data out of it.

=item C<commit>, C<rollback>

These just call the DBI methods and Do What You Mean.

=item C<disconnect>

Does What You Mean. Also note that the connection is automatically
terminated when the object is destroyed (C<undef $db> to do so
explicitly), and that all statements are also finished when the
object is destoryed. disconnect() Does not destory the object.

=back

=head2 DBIx::Simple::Result object methods

=over 10

=item C<new>

The constructor should only be called internally, by DBIx::Simple
itself. Some simple minded garbage collection is done in
DBIx::Simple, and you shouldn't be directly creating your own result
objects. The curious are encouraged to read the module's source code
to find out what the arguments to new() are.

=item C<list>

list() Returns a list of elements in a single row. This is like a
dereferenced C<$result->array()>.

=item C<array> and C<hash>

These methods return a single row, in an array reference, or a hash
reference, respectively.  Internally, fetchrow_arrayref or
fetchrow_hashref is used.

=item C<flat>

flat() Returns a list of all returned fields, flattened. This can be
very useful if you select a single column. Consider flat to be 
list()'s plural.

=item C<arrays> and C<hashes>

These methods return a list of rows of array or hash references.
Internally, fetchall_arrayref is dereferenced, or a lot of
fetchrow_hashref returns are accumulated.

=item C<map_arrays(column number)> and C<map_hashes(column name)>

These methods build a hash, with the chosen column as keys, and the
remaining columns in array or hash references as values. For
C<map_arrays>, the column number is optional and defaults to 0 (the
first column). The methods return a reference to the built hash.

=item C<map>

Returns a reference to a hash that was built using the first two 
columns as key/value pairs. Use this only if your query returns two
values per row (other values will be discarded).

=item C<rows>

Returns the number of rows.

=item finish?

There is no finish method. To finish the statement, just let the
object go out of scope (you should always use "C<my>", and
"C<use strict>") or destroy it explicitly using C<undef $result>.

=back

=head1 FEEDBACK

This module has a very low version number for a reason. I'd like to
hear from you what you think about DBIx::Simple, and if it has made
your life easier :). If you find serious bugs, let me know.  If you
think an important feature is missing, let me know (but I'm not
going to implement functions that aren't used a lot, or that are
only for effeciency, because this module has only one goal:
simplicity).

=head1 BUGS

Nothing is perfect, but let's try to create perfect things. Of
course, this module shares all DBI bugs. If you want to report a
bug, please try to find out if it's DBIx::Simple's fault or DBI's
fault first, and don't report DBI bugs to me.

Note: the map functions do not check if the key values are unique. If
they are not, keys are overwritten.

=head1 USE THIS MODULE AT YOUR OWN RISK

No warranty, no guarantees. I hereby disclaim all responsibility for
what might go wrong.

=head1 AUTHOR

Juerd <juerd@juerd.nl>

=head1 SEE ALSO

L<DBI>

=cut

