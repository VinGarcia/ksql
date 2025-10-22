# Welcome to the Keep It Simple Query Builder

This is the Keep It Simple query builder created to work
either in conjunction or separated from the KSQL package.

This package was started after KSQL and while the KSQL is already
in a usable state I still don't recommend using this one since this
being actively implemented and might change without further warning.

## Enabling kbuilder

kbuilder is only available using a specific build tag, so if you want
to experiment with this tool you will need to import KSQL normally, and when
you compile your project you will need to add the tag `ksql_enable_kbuilder_experiment`,
e.g.:


```
go run -tags ksql_enable_kbuilder_experiment [path to your entrypoint]
go test -tags ksql_enable_kbuilder_experiment [path to your entrypoint]
go build -tags ksql_enable_kbuilder_experiment [path to your entrypoint]
```

Not enabling this flag explicitly will cause it to panic, as I
don't want anyone using it without knowing it is still experimental.

## TODO List

- Add support to Update and Delete operations
- Improve support to JOINs by adding the `tablename` tag to the structs
- Add error check for when the Select, Insert and Update attrs are all empty
