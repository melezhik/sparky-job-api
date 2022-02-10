use HTTP::Tiny;
use JSON::Tiny;
use Sparrow6::DSL;

class Sparky::JobApi {

  has Str  $.api;
  has Str  $.project;
  has Str  $.job-id = "{('a' .. 'z').pick(20).join('')}.{$*PID}";
  has Int  $.workers = 4;
  has Bool $.mine = False; 

  submethod TWEAK {

    if $!mine {

      die "can't use api parameter when mine is True" if $!api;

      $!project = tags()<SPARKY_PROJECT>;

      $!job-id = tags()<SPARKY_JOB_ID>;
      
    } elsif ! $!project {

      my $i = (^$!workers).pick(1).join("")+1;

      $!project = "{tags()<SPARKY_PROJECT>}.spawned_%.2d".sprintf($i);

    }

  }

  method !proto() {
    return tags()<SPARKY_USE_TLS> ?? "https" !! "http";
  }
 
  method !internal-api() {

    my $api;

    if tags()<SPARKY_WORKER> eq "localhost" {
      $api = "{self!proto()}://127.0.0.1:{tags()<SPARKY_TCP_PORT>}";
    } elsif tags()<SPARKY_WORKER> eq "docker" {
      $api = "{self!proto()}://host.docker.internal:{tags()<SPARKY_TCP_PORT>}";
    } else {
      die "Sparky::JobApi is not supported for this type of worker: {tags()<SPARKY_WORKER>}"
    }

    return $api;

  }

  method !sparky-api() {

    return self.api || self!internal-api();

  }

  method info() {
    return { 
      project => $.project, 
      job-id => $.job-id, 
      status-url => "{self!sparky-api}/status/{$.project}/{$.job-id}", 
    }
  }

  method queue(%config) {

    die "can'r queue already running project" if $.mine;

    %config<parent-project> = tags()<SPARKY_PROJECT>;

    %config<parent-job-id> = tags()<SPARKY_JOB_ID>;

    %config<project> = $.project;
    %config<job-id> = $.job-id;

    my %c = config();

    use MONKEY-SEE-NO-EVAL;

    my %upload = %(
      config => %config,
      sparrowfile => $*PROGRAM.IO.slurp,
      sparrowdo-config => %c,
      trigger => EVAL(self!get-trigger(tags()<SPARKY_PROJECT>,tags()<SPARKY_JOB_ID>)),
    );

    my $sparky-api = self!sparky-api();

    say "send request: POST {$sparky-api}/queue ...";

    my %headers = content-type => 'application/json';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $r = HTTP::Tiny.post: "{$sparky-api}/queue", 
      headers => %headers,
      content => to-json(%upload);

    $r<status> == 200 or die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}";

    return;

  }


  method !get-trigger($project,$job-id) {

    my %headers = content-type => 'text/plain';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $r = HTTP::Tiny.get: "{self!internal-api}/trigger/{$project}/{$job-id}",
      headers => %headers;

    $r<status> == 200 or die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}";

    return $r<content>.decode;

  }
  
  method status() {

    my %r = HTTP::Tiny.get: "{self!sparky-api}/status/{$.project}/{$.job-id}";

    return "UNKNOWN" unless %r<status> == 200;

    if %r<content>.decode.Int == 1 {
      return "OK";
    } elsif %r<content>.decode.Int == -1 {
      return "FAIL";
    } elsif %r<content>.decode.Int == 0 {
      return "RUNNING";
    } elsif %r<content>.decode.Int == -2 {
      return "QUEUED";
    } else {
      return "UNKNOWN";
    }

  }

  method put-stash($data) {

    my %upload = %( 
      config => %(
        project => $.project,
        job-id => $.job-id,
      ),
      data => $data,
    );

    my $sparky-api = self!sparky-api();

    say "send request: POST {$sparky-api}/stash ...";

    my %headers = content-type => 'application/json';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $r = HTTP::Tiny.post: "{$sparky-api}/stash", 
      headers => %headers,
      content => to-json(%upload);

    $r<status> == 200 or die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}";

    return;

  }

  method get-stash() {

    my $sparky-api = self!sparky-api();

    say "send request: GET {$sparky-api}/stash/{$.project}/{$.job-id} ...";

    my %headers = content-type => 'application/json';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $r = HTTP::Tiny.get: "{$sparky-api}/stash/{$.project}/{$.job-id}",
      headers => %headers;

    $r<status> == 200 or die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}";

    return from-json($r<content>.decode);

  }

}

role Sparky::JobApi::Role {

    method wait-jobs (@q, %args?) {

      my @jobs;

      my $to = %args<timeout> || 2;

      for @q -> $j {
       my $s = supply {
          while True {
            my %out = $j.info; %out<status> = $j.status;
            emit %out;
            done if $j.status eq "FAIL" or $j.status eq "OK";
            sleep($to);
          }
        }
      $s.tap( -> $v {
          say $v if %args<debug>;
          push @jobs, $v if $v<status> eq "FAIL" or $v<status> eq "OK"
        }
      );
    }

      return %(
        "OK" => @jobs.grep({$_<status> eq "OK"}).elems,
        "FAIL" => @jobs.grep({$_<status> eq "FAIL"}).elems,
      )
  }

  method wait-job ($q, %args?) {
    self.wait-jobs(($q,),%args);
  }

  method new-job (:$api?, :$project?, :$job-id?, :$mine?) {
    my %h = (:$api, :$project, :$job-id, :$mine).grep({$_.value.defined});
    say %h.perl;
    Sparky::JobApi.new: |%h
  }

}
