Work-in-progress IO agnostic postgres client

Incomplete TODO list:

* Tests (Lots of tests)
* better error handling
* add functions that'll clean up by closing connections after finishing a task
* support binary format for parameters
* Support additional auth methods (only MD5 is implemented at the moment)
* Support SSL/TLS connections
* Add parser/serializer for all postgres backend/frontend messages
* add libraries that provide a higher level lwt/async interface
* add wrapper to handle parameters in an easier manner
* documentation

Notes:

Documentation used for this implementation: https://www.postgresql.org/docs/12/protocol.html
