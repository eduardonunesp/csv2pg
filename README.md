# csv2pg

Import `CSV` files into `Postgres` database

## Install

```bash
go install github.com/eduardonunesp/csv2pg@latest
```

## Usage

```bash
Usage:
  csv2pg <csv file> [flags]

Flags:
  -B, --db string          postgres database
  -d, --delimiter string   csv delimiter char (default ",")
  -f, --force              force command to run and drop table if needed
  -h, --help               help for csv2pg
  -H, --host string        postgres host (default "localhost")
  -W, --passwd string      postgres user password (default ",")
  -P, --port string        postgres port (default "5432")
  -S, --schema string      postgres schema (default "public")
  -M, --sslmode string     postgres SSL mode (default "disable")
  -U, --user string        postgres user (default "postgres")
  -v, --verbose            verbose output
```

Example

```bash
# delimiter is -d ";"
# host is -H 192.168.99.100
# user is -U postgres 
# database is -B database
# also you can pass the flag -W for the password

$ csv2pg users.csv -d ";" -H 192.168.99.100 -U postgres -B mydatabase
```

## LICENSE
Copyright (c) 2015, Eduardo Nunes Pereira
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of sslb nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
