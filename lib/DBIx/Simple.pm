use 5.006;
use strict;
use DBI;
use Data::Swap ();
use Carp ();

$DBIx::Simple::VERSION = '1.21';
$Carp::Internal{$_} = 1
    for qw(DBIx::Simple DBIx::Simple::Result DBIx::Simple::DeadObject);

my $quoted         = qr/'(?:\\.|[^\\']+)*'|"(?:\\.|[^\\"]+)*"/s;
my $queryfoo       = qr/(?: [^()"']+ | (??{$quoted}) | \( (??{$queryfoo}) \) )*/x;
my $subquery_match = qr/\(\s*(select\s+$queryfoo)\)/i;

my %statements;       # "$db" => { "$st" => $st, ... }
my %old_statements;   # "$db" => [ [ $query, $st ], ... ]
my %keep_statements;  # "$db" => $int

my $err_message = '%s no longer usable (because of %%s)';
my $err_cause   = '%s at %s line %d';

package DBIx::Simple;

### private helper subs

sub _dummy { bless \my $dummy, 'DBIx::Simple::Dummy' }

### constructor

sub connect {
    my ($class, @arguments) = @_;
    my $self = { omniholder => '(??)', emulate_subqueries => 0, lc_columns => 1 };
    if (defined $arguments[0] and UNIVERSAL::isa($arguments[0], 'DBI::db')) {
	$self->{dbh} = shift @arguments;
	Carp::carp("Additional arguments for $class->connect are ignored")
	    if @arguments;
    } else {
	$arguments[3]->{PrintError} = 0
	    unless defined $arguments[3] and defined $arguments[3]{PrintError};
	$self->{dbh} = DBI->connect(@arguments);
    }
    
    return undef unless $self->{dbh};
    
    bless $self, $class;
    
    $statements{$self}      = {};
    $old_statements{$self}  = [];
    $keep_statements{$self} = 16;
    
    return $self;
}

sub new {
    my ($class) = shift;
    $class->connect(@_);
}

### properties

sub omniholder         : lvalue { $_[0]->{omniholder} }
sub emulate_subqueries : lvalue { $_[0]->{emulate_subqueries} }
sub esq                : lvalue { $_[0]->{emulate_subqueries} }
sub keep_statements    : lvalue { $keep_statements{ $_[0] } }
sub lc_columns         : lvalue { $_[0]->{lc_columns} }

### private methods

# Replace (??) with (?, ?, ?, ...)
sub _replace_omniholder {
    my ($self, $query, $binds) = @_;
    my $omniholder = $self->{omniholder};
    
    if (defined $omniholder and length $omniholder) {
        return if $$query !~ /\Q$omniholder/;
	my $omniholders = 0;
	my $re = quotemeta $omniholder;
	$$query =~ s[($re|$quoted+|(?:(?!$re).)+)] {
	    $1 eq $omniholder
	    ? do {
		Carp::croak('There can be only one omniholder')
		    if $omniholders++;
                
		'(' . join(', ', ('?') x @$binds) . ')'
	    }
	    : $1
	}eg;
    }
}   

# Deprecated
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

sub _die {
    my ($self, $cause) = @_;

    defined and $_->_die($cause, 0)
        for values %{ $statements{$self} }, 
        map $$_[1], @{ $old_statements{$self} };
    delete $statements{$self};
    delete $old_statements{$self};
    delete $keep_statements{$self};

    # Conditional, because destruction order is not guaranteed
    # during global destruction.
    $self->{dbh}->disconnect() if defined $self->{dbh};

    Data::Swap::swap(
        $self,
        bless {
            what  => 'Database object',
            cause => $cause
        }, 'DBIx::Simple::DeadObject'
    ) unless $cause =~ /DESTROY/;  # Let's not cause infinite loops :)
}

### public methods

sub query {
    my ($self, $query, @binds) = @_;
    $self->{success} = 0;
   
    $self->_replace_omniholder(\$query, \@binds);
    $self->_interpolate_subqueries(\$query, \@binds) or return _dummy
        if $self->{emulate_subqueries};
    
    my $st;
    my $sth;
   
    my $old = $old_statements{$self};

    if (my $i = (grep $old->[$_][0] eq $query, 0..$#$old)[0]) {
        $st = splice(@$old, $i, 1)->[1];
        $sth = $st->{sth};
    } else {
        eval { $sth = $self->{dbh}->prepare($query) } or do {
            if ($@) {
                $@ =~ s/ at \S+ line \d+\.\n\z//;
                Carp::croak($@);
            }
            $self->{reason} = "Prepare failed ($DBI::errstr)";
            return _dummy;
        };

        # $self is quoted on purpose, to pass along the stringified version,
        # and avoid increasing reference count.
        $st = bless {
            db    => "$self",
            sth   => $sth,
            query => $query
        }, 'DBIx::Simple::Statement';
        $statements{$self}{$st} = $st;
    }

    eval { $sth->execute(@binds) } or do {
        if ($@) {
            $@ =~ s/ at \S+ line \d+\.\n\z//;
            Carp::croak($@);
        }

        $self->{reason} = "Execute failed ($DBI::errstr)";
	return _dummy;
    };

    $self->{success} = 1;

    return bless { st => $st, lc_columns => $self->{lc_columns} }, 'DBIx::Simple::Result';
}

sub error {
    my ($self) = @_;
    return 'DBI error: ' . (ref $self ? $self->{dbh}->errstr : $DBI::errstr);
}

sub dbh            { $_[0]->{dbh}             }
sub begin_work     { $_[0]->{dbh}->begin_work }
sub commit         { $_[0]->{dbh}->commit     }
sub rollback       { $_[0]->{dbh}->rollback   }
sub func           { $_[0]->{dbh}->func(@_[1..$#_]) }

sub last_insert_id {
    my ($self) = @_;

    ($self->{dbi_version} ||= DBI->VERSION) >= 1.38 or Carp::croak(
    	"DBI v1.38 required for last_insert_id" .
	"--this is only $self->{dbi_version}, stopped"
    );
    
    return $self->{dbh}->last_insert_id(@_[1..$#_]);
}

sub disconnect {
    my ($self) = @_;
    $self->_die(sprintf($err_cause, "$self->disconnect", (caller)[1, 2]));
}

sub DESTROY {
    my ($self) = @_;
    $self->_die(sprintf($err_cause, "$self->DESTROY", (caller)[1, 2]));
}

package DBIx::Simple::Dummy;

use overload
    '""' => sub { shift },
    bool => sub { 0 };

sub new      { bless \my $dummy, shift }
sub AUTOLOAD { return }

package DBIx::Simple::DeadObject;

sub _die {
    my ($self) = @_;
    Carp::croak(
        sprintf(
            "(This should NEVER happen!) " . 
            sprintf($err_message, $self->{what}),
            $self->{cause}
        )
    );
}

sub AUTOLOAD {
    my ($self) = @_;
    Carp::croak(
        sprintf(
            sprintf($err_message, $self->{what}),
            $self->{cause}
        )
    );
}
sub DESTROY { }

package DBIx::Simple::Statement;

sub _die {
    my ($self, $cause, $save) = @_;
    
    $self->{sth}->finish() if defined $self->{sth};
    $self->{dead} = 1;

    my $stringy_db = "$self->{db}";
    my $stringy_self = "$self";
    
    my $foo = bless {
        what  => 'Statement object',
        cause => $cause
    }, 'DBIx::Simple::DeadObject';
    
    Data::Swap::swap($self, $foo);
    
    my $old = $old_statements{ $foo->{db} };
    my $keep = $keep_statements{ $foo->{db} };

    if ($save and $keep) {
        $foo->{dead} = 0;
        shift @$old until @$old + 1 <= $keep;
        push @$old, [ $foo->{query}, $foo ];
    }

    delete $statements{ $stringy_db }{ $stringy_self };
}

sub DESTROY {
    # This better only happen during global destruction...
    return if $_[0]->{dead};
    $_[0]->_die('Ehm', 0); 
}    

package DBIx::Simple::Result;

sub _die {
    my ($self, $cause) = @_;
    if ($cause) {
        $self->{st}->_die($cause, 1);
        Data::Swap::swap(
            $self,
            bless {
                what  => 'Result object',
                cause => $cause,
            }, 'DBIx::Simple::DeadObject'
        );
    } else {
        $cause = $self->{st}->{cause};
        Data::Swap::swap(
            $self,
            bless {
                what  => 'Result object',
                cause => $cause
            }, 'DBIx::Simple::DeadObject'
        );
        Carp::croak(
            sprintf(
                sprintf($err_message, $self->{what}),
                $cause
            )
        );
    }
}

sub func { $_[0]->{st}->{sth}->func(@_[1..$#_]) }
sub attr { my $dummy = $_[0]->{st}->{sth}->{$_[1]} }

sub columns {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my $c = $_[0]->{st}->{sth}->{ $_[0]->{lc_columns} ? 'NAME_lc' : 'NAME' };
    return wantarray ? @$c : $c;
}

sub bind {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    $_[0]->{st}->{sth}->bind_columns(\@_[1..$#_]);
}

sub into {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my $sth = $_[0]->{st}->{sth};
    $sth->bind_columns(\@_[1..$#_]) if @_ > 1;
    return $sth->fetch;
}
*fetch = \&into;  # This class isn't subclassable anyway.

sub list {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    return $_[0]->{st}->{sth}->fetchrow_array if wantarray;
    return($_[0]->{st}->{sth}->fetchrow_array)[-1];
}

sub array {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my $row = $_[0]->{st}->{sth}->fetchrow_arrayref or return; 
    return [ @$row ];
}

sub hash {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    return $_[0]->{st}->{sth}->fetchrow_hashref(
        $_[0]->{lc_columns} ? 'NAME_lc' : 'NAME'
    );
}

sub flat {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    return map @$_, $_[0]->arrays;
}

sub arrays {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    return wantarray
        ? @{ $_[0]->{st}->{sth}->fetchall_arrayref }
        :    $_[0]->{st}->{sth}->fetchall_arrayref;
}

sub hashes {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self) = @_;
    my @return;
    my $dummy;
    push @return, $dummy while $dummy = $self->hash;
    return wantarray ? @return : \@return;
}

sub map_hashes {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self, $keyname) = @_;
    Carp::croak('Key column name not optional') if not defined $keyname;
    my @rows = $self->hashes;
    my @keys;
    push @keys, delete $_->{$keyname} for @rows;
    my %return;
    @return{@keys} = @rows;
    return wantarray ? %return : \%return;
}

sub map_arrays {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self, $keyindex) = @_;
    $keyindex += 0;
    my @rows = $self->arrays;
    my @keys;
    push @keys, splice @$_, $keyindex, 1 for @rows;
    my %return;
    @return{@keys} = @rows;
    return wantarray ? %return : \%return;
}

sub map {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self) = @_;
    my %return = map { $_->[0] => $_->[1] } $self->arrays;
    return wantarray ? %return : \%return;
}

sub rows {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self) = @_;
    return $self->{st}->{sth}->rows;
}

sub finish {
    $_[0]->_die if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self) = @_;
    $self->_die(
        sprintf($err_cause, "$self->finish", (caller)[1, 2])
    );
}

sub DESTROY {
    return if ref $_[0]->{st} eq 'DBIx::Simple::DeadObject';
    my ($self) = @_;
    $self->_die(
        sprintf($err_cause, "$self->DESTROY", (caller)[1, 2])
    );
}

1;

__END__

=head1 NAME

DBIx::Simple - Easy-to-use OO interface to DBI, capable of emulating subqueries

=head1 SYNOPSIS

=head2 DBIx::Simple
 
    $db = DBIx::Simple->connect(...)  # or ->new

    $db->omniholder = '(??)'
    $db->emulate_subqueries = 0  # or ->esq 
    $db->keep_statements = 16
    $db->lc_columns = 1

    $db->begin_work         $db->commit
    $db->rollback           $db->disconnect
    $db->func(...)          $db->last_insert_id

    $result = $db->query(...)

=head2 DBIx::Simple::Result

    $result->into($foo, $bar, $baz)
    $row = $result->fetch
    
    @row = $result->list    @rows = $result->flat
    $row = $result->array   @rows = $result->arrays
    $row = $result->hash    @rows = $result->hashes

    %map = $result->map_arrays(...)
    %map = $result->map_hashes(...)
    %map = $result->map

    $rows = $result->rows

    $result->finish

=head2 Examples

Please read L<DBIx::Simple::Examples> for code examples.

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
$db->query(...) or die $db->error >>.

=head2 DBIx::Simple methods

=over 4

=item C<< DBIx::Simple->connect($dbh) >>

=item C<< DBIx::Simple->connect($dsn, $user, $pass, \%options) >>

=item C<< DBIx::Simple->new($dbh) >>

=item C<< DBIx::Simple->new($dsn, $user, $pass, \%options) >>

The C<connect> or C<new> class method takes either an existing DBI object
($dbh), or a list of arguments to pass to C<< DBI->connect >>. See L<DBI> for a
detailed description.

You cannot use this method to clone a DBIx::Simple object: the $dbh passed
should be a DBI::db object, not a DBIx::Simple object.

This method is the constructor and returns a DBIx::Simple object on success. On
failure, it returns undef.

=item C<omniholder = $string>

B<This method is deprecated and will be removed in a future version.>
In future versions, the omniholder will always be C<(??)> and no longer be
user definable. If you have a good reason to not want C<(??)>, please do try to
convince me.

The omniholder string is, when found in a query, replaced with C<(?, ?, ?,
...)> with as many question marks as C<@values> passed to C<query>.

=item C<emulate_subqueries = $bool>

=item C<esq = $bool>

B<This method is deprecated and will be removed in a future version.>
C<emulate_subqueries> was originally invented because MySQL had no subselects
of its own, but it has now.

When true at time of query execution, makes C<query> emulate nested subqueries
(SELECT only) by executing them and interpolating the results.
C<emulate_subqueries> is false by default and should not be used if the
database provides real subqueries.

Only subqueries like C<(SELECT ...)> (note the parentheses) are interpolated.

Please note that emulation is done by doing multiple queries and is not
atomic, as it would be if the database supported real subqueries. The
queries are executed independently.

=item C<lc_columns = $bool>

When true at time of query execution, makes C<columns>, C<hash>, C<new_hash>,
C<hashes>, and C<map_hashes> use lower cased column names. C<lc_columns> is
true by default.

=item C<keep_statements = $integer>

Sets the number of statement objects that DBIx::Simple can keep for reuse. This
can dramatically speed up repeated queries (like when used in a loop).
C<keep_statements> is 16 by default. 

A query is only reused if it equals a previously used one literally. This means
that to benefit from this caching mechanism, you must use placeholders and
never interpolate variables yourself.

    # Wrong:
    $db->query("INSERT INTO foo VALUES ('$foo', '$bar', '$baz')");
    $db->query("SELECT FROM foo WHERE foo = '$foo' OR bar = '$bar'");

    # Right:
    $db->query('INSERT INTO foo VALUES (??)', $foo, $bar, $baz);
    $db->query('SELECT FROM foo WHERE foo = ? OR bar = ?', $foo, $baz);

Of course, automatic value escaping is a much better reason for using
placeholders.

=item C<error>

Returns the error string of the last DBI method. See the discussion of "C<err>"
and "C<errstr>" in L<DBI>.

=item C<query($query, @values)>

The C<query> method prepares and executes the query and returns a result object.

If an omniholder (see above) is present in the query, it is replaced with a
list of as many question marks as @values. If subquery emulation (see above) is
enabled, subquery results are interpolated in the main query before the main
query is executed.

The database drivers substitute placeholders (question marks that do not appear
in quoted literals) in the query with the given @values, after them escaping
them. You should always use placeholders, and never use user input in database
queries.

On success, returns a DBIx::Simple::Result object.

On failure, returns a DBIx::Simple::Dummy object.

=item C<begin_work>, C<commit>, C<rollback>

These transaction related methods call the DBI respective methods and
Do What You Mean. See L<DBI> for details.

=item C<func(...)>

This calls the C<func> method of DBI. See L<DBI> for details.

=item C<last_insert_id(...)>

This calls the C<last_insert_id> method of DBI. See L<DBI> for details. Note
that this feature requires DBI 1.38 or newer.

=item C<dbh>

Exposes the internal database handle. Use this only if you know what you are
doing. Keeping a reference or doing queries can interfere with DBIx::Simple's
garbage collection and error reporting.

=item C<disconnect>

Destroys (finishes) active statements and disconnects. Whenever the database
object is destroyed, this happens automatically. After disconnecting, you can
no longer use the database object or any of its result objects.

=back

=head2 DBIx::Simple::Dummy

The C<query> method of DBIx::Simple returns a dummy object on failure. Its
methods all return an empty list or undef, depending on context. When used in
boolean context, a dummy object evaluates to false.

=head2 DBIx::Simple::Result methods

=over 12

=item C<columns>

Returns a list of column names. In scalar context, returns an array reference.

Column names are lower cased if C<lc_underscores> was true when the query was
executed.

=item C<bind(LIST)>

Binds the given LIST to the columns. The elements of LIST must be writable
LVALUEs. In other words, use this method as:

    $result->bind(my ($foo, $bar));
    $result->fetch;

Or, combined:

    $result->into(my ($foo, $bar));

Unlike with DBI's C<bind_columns>, the C<\> operator is not needed.

Bound variables are very efficient. Binding a tied variable doesn't work.

=item C<fetch>

Fetches a single row and returns a reference to the array that holds the
values. This is the same array every time.

Subsequent fetches (using any method) may change the values in the variables
passed and the returned reference's array.

=item C<into(LIST)>

Combines C<bind> with C<fetch>. Returns what C<fetch> returns.

=item C<list>

Fetches a single row and returns a list of values. In scalar context, 
returns only the last value.

=item C<array>

Fetches a single row and returns an array reference.

=item C<hash>

Fetches a single row and returns a hash reference.

Keys are lower cased if C<lc_underscores> was true when the query was executed.

=item C<flat>

Fetches all remaining rows and returns a flattened list.

=item C<arrays>

Fetches all remaining rows and returns a list of array references.

In scalar context, returns an array reference.

=item C<hashes>

Fetches all remaining rows and returns a list of hash references. 

In scalar context, returns an array reference.

Keys are lower cased if C<lc_underscores> was true when the query was executed.

=item C<map_arrays($column_number)>

Constructs a hash of array references keyed by the values in the chosen column.

In scalar context, returns a hash reference.

=item C<map_hashes($column_name)>

Constructs a hash of hash references keyed by the values in the chosen column.

In scalar context, returns a hash reference.

=item C<map>

Constructs a simple hash, using the first two columns as key/value pairs.
Should only be used with queries that return two columns.

In scalar context, returns a hash reference.

=item C<rows>

Returns the number of rows affected by the last row affecting command, or -1 if
the number of rows is not known or not available.

For SELECT statements, it is generally not possible to know how many rows are
returned. MySQL does provide this information. See L<DBI> for a detailed
explanation.

=item C<attr(...)>

Returns a copy of an sth attribute (property). See L<DBI/"Statement Handle
Attributes"> for details.

=item C<func(...)>

This calls the C<func> method of DBI. See L<DBI> for details.

=item C<finish>

Finishes the statement. After finishing a statement, it can no longer be used.
When the result object is destroyed, its statement handle is automatically
finished and destroyed. There should be no reason to call this method
explicitly; just let the result object go out of scope.

=back

=head1 MISCELLANEOUS

Although this module has been tested thoroughly in production environments, it
still has no automated test suite. If you want to write tests, please contact
me.

The mapping methods do not check whether the keys are unique. Rows that are
fetched later overwrite earlier ones.

PrintError is disabled by default. If you enable it, beware that it will report
line numbers in DBIx/Simple.pm. 

Note: this module does not provide any SQL abstraction and never will. If you
don't want to write SQL queries, use DBIx::Abstract.

=head1 LICENSE

There is no license. This software was released into the public domain. Do with
it what you want, but on your own risk. The author disclaims any
responsibility.

=head1 AUTHOR

Juerd Waalboer <juerd@cpan.org> <http://juerd.nl/>

=head1 SEE ALSO

L<perl>, L<perlref>, L<DBI>, L<DBIx::Simple::Examples>

=cut

