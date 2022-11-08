# cefasRBR

This R package is a set of tools for working with `EasyParse` data from RBR CTD profiling instruments.

https://rbr-global.com/products/standard-loggers/

## Installation

The package can be installed directly from github using the remotes package:

```r
remotes::install_github("CefasRepRes/cefasRBR")
```

## Related packages

This package takes a different approach from the Dan Kelly's `oce` package (https://dankelley.github.io/oce/).
We aim to keep the underlying structure similar to the .RSK SQLite files rather than coerce the data to another class (i.e. the `CTD` class in `oce`).
