# NanoCSV, Faster C++11 multithreaded header-only CSV parser

NanoCSV is a fater C++11 multithreaded header-only CSV parser.

## Compiler options

* NANOCSV_NO_IO : Disable I/O(file access, stdio, mmap).

## TODO

* [ ] Robust error handling.
* [ ] Support different number of fields among records;
* [ ] Parse complex value(e.g. `3.0 + 4.2j`)
* [ ] Parse some special values, for example `#INF`, `#NAN`.
* [ ] CSV writer.

## References

* RFC 4180 https://www.ietf.org/rfc/rfc4180.txt

## License

MIT License

### Third-party license

* stack_container : Copyright (c) 2006-2008 The Chromium Authors. BSD-style license.

