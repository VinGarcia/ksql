# Welcome to the KISS Query Builder

This is the Keep It Stupid Simple query builder created to work
either in conjunction or separated from the ksql package.

This package was started after ksql and while the ksql is already
in a usable state I still don't recommend using this one since this
being actively implemented and might change without further warning.

## TODO List

- Add support to Update and Delete operations
- Improve support to JOINs by adding the `tablename` tag to the structs
- Add error check for when the Select, Insert and Update attrs are all empty
