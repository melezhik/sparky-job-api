use HTTP::Tiny;
use JSON::Tiny;
use Sparrow6::DSL;

class Sparky::JobApi {

  has Str  $.api;
  has Str  $.project;
  has Str  $.job-id = "{('a' .. 'z').pick(20).join('')}.{$*PID}";
  has Int  $.workers = 4;

  submethod TWEAK {

   unless $!project  {
      my $i = (^$!workers).pick(1).join("")+1;
      $!project = "{tags()<SPARKY_PROJECT>}.spawned_%.2d".sprintf($i);
    }

  }


  method !sparky-api() {

    my $sparky-api;

    if self.api {
      $sparky-api = self.api;
    } elsif tags()<SPARKY_WORKER> eq "localhost" {
      $sparky-api = "http://127.0.0.1:{tags()<SPARKY_TCP_PORT>}";
    } elsif tags()<SPARKY_WORKER> eq "docker" {
      $sparky-api = "http://host.docker.internal:{tags()<SPARKY_TCP_PORT>}";
    } else {
      die "Sparky::JobApi is not supported for this type of worker: {tags()<SPARKY_WORKER>}"
    }

    return $sparky-api;

  }

  method info() {
    return { 
      project => $.project, 
      job-id => $.job-id, 
      status-url => "{self!sparky-api}/status/{$.project}/{$.job-id}", 
    }
  }

  method queue(%config) {

    %config<parent-project> = tags()<SPARKY_PROJECT>;

    %config<parent-job-id> = tags()<SPARKY_JOB_ID>;

    %config<project> = $.project;
    %config<job-id> = $.job-id;

    my %c = config();

    my %upload = %(
      config => %config,
      sparrowfile => $*PROGRAM.IO.slurp,
      sparrowdo-config => %c,
    );

    my $sparky-api = self!sparky-api();

    say "send request to {$sparky-api}/queue ...";

    my $r = HTTP::Tiny.post: "{$sparky-api}/queue", 
      headers => { content-type => 'application/json' },
      content => to-json(%upload);

    $r<status> == 200 or die $r.perl;

    return;

  }

  
  method status() {

    my %r = HTTP::Tiny.new: "{self!sparky-api}/status/{$.project}/{$.job-id}";

    return "UNKNOWN" unless %r<status> == 200;

    if %r<content>.Int == 1 {
      return "OK";
    } elsif %r<content>.Int == -1 {
      return "FAIL";
    } elsif %r<content>.Int == 0 {
      return "RUNNING";
    } elsif %r<content>.Int == -2 {
      return "QUEUED";
    }

  }

}

