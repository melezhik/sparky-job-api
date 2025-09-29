use HTTP::Tiny;

use JSON::Fast;

use Sparrow6::DSL;

unit module Sparky::JobApi:ver<0.0.8>;

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

    my $trigger = EVAL(self!get-trigger(tags()<SPARKY_PROJECT>,tags()<SPARKY_JOB_ID>));
    my %upload = %(
      config => %config,
      sparrowfile => $*PROGRAM.IO.slurp,
      sparrowdo-config => %c,
      trigger => $trigger,
    );

    my $sparky-api = self!sparky-api();

    say ">>> send request: POST job to {$sparky-api}/queue | trigger: {$trigger.perl} | config: {%config.perl}...";

    my %headers = content-type => 'application/json';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $cnt = 0;

    while True {
      my $r = HTTP::Tiny.post: "{$sparky-api}/queue", 
        headers => %headers,
        content => to-json(%upload);
        last if $r<status> == 200;
        if $cnt == 3 or $r<status> != 599 {
          die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}" 
        }
        $cnt++;
        say ">>> (599 recieved) http retry: #0{$cnt}";
        sleep(60);
    }
    return;
  }


  method !get-trigger($project,$job-id) {

    my %headers = content-type => 'text/plain';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $return;
    my $cnt = 0;
    while True {
      my $r = HTTP::Tiny.get: "{self!internal-api}/trigger/{$project}/{$job-id}",
        headers => %headers;
        if $r<status> == 200 {
          $return = $r<content>.decode;
          last;
        }
        if $cnt == 3 or $r<status> != 599 {
          die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}"
        }
        $cnt++;
        say ">>> (599 recieved) http retry: #0{$cnt}";
        sleep(60);
    }

    return $return;

  }
  
  method status() {


    # try to use file for localhost

    if self!sparky-api ~~ /'127.0.0.1'/ {

      if "{%*ENV<HOME>}/.sparky/work/{$.project}/.states/{$.job-id}".IO ~~ :f {
       my $s = "{%*ENV<HOME>}/.sparky/work/{$.project}/.states/{$.job-id}".IO.slurp;
       if $s == 1 {
        return "OK";
       } elsif $s == -1 {
        return "FAIL";
       } elsif $s == -11 {
        return "FAIL";
       } elsif $s == 0 {
        return "RUNNING";
       } elsif $s == -2 {
        return "QUEUED";
       } else {
        return "UNKNOWN";
       }
      } elsif "{%*ENV<HOME>}/.sparky/projects/{$.project}/.triggers/{$.job-id}".IO ~~ :f {
        return "QUEUED"
      }
    }

    my %r = HTTP::Tiny.get: "{self!sparky-api}/status/{$.project}/{$.job-id}";

    return "UNKNOWN" unless %r<status> == 200;

    if %r<content>.decode.Int == 1 {
      return "OK";
    } elsif %r<content>.decode.Int == -1 {
      return "FAIL";
    } elsif %r<content>.decode.Int == -11 {
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

    say ">>> send request: POST stash data to {$sparky-api}/stash ...";

    my %headers = content-type => 'application/json';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $cnt = 0;

    while True {
      my $r = HTTP::Tiny.post: "{$sparky-api}/stash",
        headers => %headers,
        content => to-json(%upload);
        if $r<status> == 200 {
          last;
        }
        if $cnt == 3 or $r<status> != 599 {
          die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}"
        }
        $cnt++;
        say ">>> (599 recieved) http retry: #0{$cnt}";
        sleep(60);
    }
    return;
  }

  method get-stash() {

    my $sparky-api = self!sparky-api();

    say ">>> send request: GET stash data from {$sparky-api}/stash/{$.project}/{$.job-id} ...";

    my %headers = content-type => 'application/json';

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $return;
    my $cnt = 0;

    while True {
      my $r = HTTP::Tiny.get: "{$sparky-api}/stash/{$.project}/{$.job-id}",
        headers => %headers;
        if $r<status> == 200 {
          $return = from-json($r<content>.decode);
          last;
        }
        if $cnt == 3 or $r<status> != 599 {
          die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}"
        }
        $cnt++;
        say ">>> (599 recieved) http retry: #0{$cnt}";
        sleep(60);
    }
    return $return;
  }

  method put-file($path,$filename) {

    my $sparky-api = self!sparky-api();

    say ">>> send request: PUT job file to {$sparky-api}/file/project/{$.project}/job/{$.job-id}/filename/{$filename}"; 
    say ">>> file path: {$path}";

    my %headers = %(
      Content-Type => "application/octet-stream"
    );

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $cnt = 0;

    while True {
      my $r = HTTP::Tiny.put: "{$sparky-api}/file/project/{$.project}/job/{$.job-id}/filename/{$filename}",
        headers => %headers,
        content => Blob.new($path.IO.slurp: :bin);
        last if $r<status> == 200;
        if $cnt == 3 or $r<status> != 599 {
          die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}"
        }
        $cnt++;
        say ">>> (599 recieved) http retry: #0{$cnt}";
        sleep(60);
    }

    return;
  }

  method get-file($filename, :$text = False) {

    my $sparky-api = self!sparky-api();

    say ">>> send request: GET job file from {$sparky-api}/file/{$.project}/{$.job-id}/{$filename} ...";

    my %headers = %(
      Content-Type => "application/octet-stream"
    );

    %headers<token> = tags()<SPARKY_API_TOKEN> if tags()<SPARKY_API_TOKEN>;

    my $return;
    my $cnt = 0;

    while True {
      my $r = HTTP::Tiny.get: "{$sparky-api}/file/{$.project}/{$.job-id}/{$filename}",
        headers => %headers;
        if $r<status> == 200 {
          if $text == True  {
            return $r<content>.decode;
          } else {
            $return = $r<content>;
          }
          last;
        }
        if $cnt == 3 or $r<status> != 599 {
          die "{$r<status>} : { $r<content> ?? $r<content>.decode !! ''}"
        }
        $cnt++;
        say ">>> (599 recieved) http retry: #0{$cnt}";
        sleep(60);
    }

    return $return;

  }

}

role Sparky::JobApi::Role {

    has Str $.stage = tags()<stage> || "main";

    method run () {
      say "run stage: {$.stage}";
      self."stage-{$.stage}"();  
    }

    method wait-jobs (@q, %args?) {

      my @jobs;

      my $to = %args<sleep> || 5;
      my $start-time = now;
      my $timeout = %args<timeout> || %*ENV<SPARKY_JOB_TIMEOUT> || 60*10;

      say ">>> wait for jobs, timeout: $timeout sec";

      for @q -> $j {
        my $s = supply {
          while True {
            my %out = $j.info; %out<status> = $j.status;
            emit %out;
            done if $j.status eq "FAIL" or $j.status eq "OK";
            sleep($to);
            my $time = now - $start-time;
            if $time > $timeout {
              %out<status> = "TIMEOUT";
              emit %out;
              done;
            }
          }
        }
        $s.tap( -> $v {
          say ">>> [{DateTime.now}] {$v.perl}" if %args<debug>;
          push @jobs, $v if $v<status> eq "FAIL" or $v<status> eq "OK" or $v<status> eq "TIMEOUT"
        }
      );
    }

      return %(
        "OK" => @jobs.grep({$_<status> eq "OK"}).elems,
        "FAIL" => @jobs.grep({$_<status> eq "FAIL"}).elems,
        "TIMEOUT" => @jobs.grep({$_<status> eq "TIMEOUT"}).elems,
      )
  }

  method wait-job ($q, %args?) {
    self.wait-jobs(($q,),%args);
  }

  method new-job (:$api?, :$project?, :$job-id?, :$mine?, :$workers? ) {
    my %h = (:$api, :$project, :$job-id, :$mine, :$workers).grep({$_.value.defined});
    #say %h.perl;
    Sparky::JobApi.new: |%h
  }

}
